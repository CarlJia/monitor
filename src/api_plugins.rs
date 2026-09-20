//! 插件的面板 HTTP 接口(U5):上传、启停删、测试、派发日志与 kv。
//! 从 api.rs 原样拆出;`Admin` 提取器与 `fail`/`bad` 响应助手仍在 api.rs,
//! 这里通过 `use crate::api::{...}` 复用。

use axum::extract::multipart::MultipartError;
use axum::extract::{Multipart, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tracing::warn;

use crate::api::{bad, fail, Admin};
use crate::db::PluginRow;
use crate::notification_bus::Event;
use crate::plugin::{self, Manifest};
use crate::{App, Shared};

// ---- plugins(U5):上传、启停删、测试、日志与 kv ----

/// 插件上传包(tar.gz)的累计字节上限(R11)。独立于主题的 32 MiB:一个插件是
/// 一份 manifest 加一个 wasm 模块,8 MiB 已是宽裕。这条路由因此挂在
/// [`MAX_CHUNK`] 的 merge 子 router 上(见 main),reverse proxy 需要放行的
/// 单请求大小与备份分片相同。
pub const MAX_PLUGIN: u64 = 8 * 1024 * 1024;

/// 解包防护,与 frontend 的主题包防护同一套思路,只是全程内存、不落盘:
/// 条目数、单个 entry 与解压后的总量分别封顶,把三种形态的解压炸弹都挡在
/// 写库之前。上限比主题宽(200 条 / 16 MiB / 32 MiB),因为插件包预期就是
/// 两个文件,任何接近上限的包都可疑。
const PLUGIN_MAX_ENTRIES: usize = 200;
const PLUGIN_MAX_FILE: u64 = 16 << 20;
const PLUGIN_MAX_EXPANDED: u64 = 32 << 20;
/// manifest 与 wasm 模块各自的体量上限:manifest 是几十行 TOML,wasm 模块
/// 在 16 MiB 封顶处与单 entry 上限重合。
const PLUGIN_MANIFEST_MAX: usize = 64 * 1024;
const PLUGIN_WASM_MAX: usize = 16 << 20;

/// 插件 handler 共用的 404 门:多数 handler 只要 plugin_id(delete 连它删的
/// kv 行也只按 plugin_id 找),所以这里读轻量的 [`Db::plugin_id_of`] 而不是
/// 整行——整行会连几 MiB 的 wasm blob 一起读出来再扔掉。返回 `Err(响应)`
/// 的两种情况:404(行不存在)与 500(读库失败)。Err 装的是现成的 Response
/// 而不是错误码,调用侧直接 return;它比一个错误码大,但这是本函数唯一的
/// 消费方式,装箱省下的那点栈不值得多一次解引用。
#[allow(clippy::result_large_err)]
fn plugin_or_404(app: &App, id: i64) -> Result<String, Response> {
    match app.db.plugin_id_of(id) {
        Ok(Some(plugin_id)) => Ok(plugin_id),
        Ok(None) => Err(StatusCode::NOT_FOUND.into_response()),
        Err(e) => Err(fail(e)),
    }
}

/// 一个插件不满足某能力声明时的 404(未声明 page/cleanup 的路由)。纯文本
/// 响应体,与 `bad`/`fail` 同一形状——面板只有一条错误路径(`res.text()`),
/// 返回 JSON 会让 toast 里出现 `{"error":"..."}` 的原文。
fn not_found(message: &str) -> Response {
    (StatusCode::NOT_FOUND, message.to_owned()).into_response()
}

/// 已加载插件的 manifest。未加载返回 400,与 `test_plugin` 对同一状况的回答
/// 一致——「插件没启用」和「插件没声明这项能力」在面板上都表现为"点了没反应",
/// 但恢复动作完全不同(去启用 vs 去改 manifest),不能混成同一个 404。
///
/// `manifest_of` 只看内存里已加载(启用)的插件,所以先判加载状态再判能力声明。
///
/// `#[allow(result_large_err)]` 与 [`plugin_or_404`] 同一理由:Err 装的是现成
/// 的 Response,调用侧直接 return,装箱省下的那点栈不值得多一次解引用。
#[allow(clippy::result_large_err)]
fn loaded_manifest(app: &App, id: i64) -> Result<Manifest, Response> {
    let registry = app.plugins.read().unwrap_or_else(|e| e.into_inner());
    if !registry.is_loaded(id) {
        return Err(bad("插件未启用或加载失败；先启用它再重试"));
    }
    registry.manifest_of(id).ok_or_else(|| bad("插件未启用或加载失败；先启用它再重试"))
}

/// Installs an uploaded plugin package (R11): a `multipart/form-data` request
/// whose `plugin` field carries the plugin's `tar.gz`.
///
/// 收字节(上限 [`MAX_PLUGIN`])在异步侧完成;解包、校验、编译与写库整体
/// 挪进 spawn_blocking——wasm 编译是百毫秒级的 CPU 工作,不该占着调度线程。
/// 一个包要么整体验证通过,要么什么都不写:写库发生在解包、manifest 校验
/// 与版本检查全部通过之后,而编译(预热校验)失败也照常入库——作者需要
/// 在面板上看到原因,而不是被迫从日志里找(KTD10:上传后默认不启用)。
///
/// 同 `plugin_id` 再次上传是**升级**:版本更高才替换,行 id、kv 与
/// `plugin_data` 原样保留(删除接口会连数据一起删,所以升级不能走「删了再传」),
/// 替换后回到停用态。版本没提高则 400,已装的那份一行都不动。
pub async fn upload_plugin(_: Admin, State(app): State<Shared>, mut multipart: Multipart) -> Response {
    // 找名为 plugin 的文件字段,边收边计数:上限检查不等包收完,多出的第一
    // 个字节就被拒绝,不用把 8 MiB 都吃进内存再丢弃。
    let mut bytes: Vec<u8> = Vec::new();
    let mut archive: Option<Vec<u8>> = None;
    while let Some(field) = match multipart.next_field().await {
        Ok(field) => field,
        Err(e) => return multipart_failed(e),
    } {
        if field.name() != Some("plugin") {
            continue; // 别的字段(比如未来的注释)收下即丢,不报错。
        }
        let mut field = field;
        loop {
            match field.chunk().await {
                Ok(Some(piece)) => {
                    bytes.extend_from_slice(&piece);
                    if bytes.len() as u64 > MAX_PLUGIN {
                        return bad(&format!("插件包超过 {} MiB 的上限", MAX_PLUGIN / 1024 / 1024));
                    }
                }
                Ok(None) => break,
                Err(e) => return multipart_failed(e),
            }
        }
        archive = Some(std::mem::take(&mut bytes));
        break; // 第一个 plugin 字段为准,重复出现的同名字段忽略。
    }
    let Some(archive) = archive else {
        return bad("缺少名为 plugin 的文件字段");
    };

    let installed = {
        let app = app.clone();
        tokio::task::spawn_blocking(move || install_plugin(&app, &archive))
    }
    .await;
    match installed.map_err(|e| anyhow::anyhow!(e)).and_then(|r| r) {
        Ok(body) => Json(body).into_response(),
        Err(e) => bad(&format!("{e:#}")),
    }
}

/// multipart 中断时的响应。超限有两条路:Multipart 提取器自己的 body limit
/// (main 在上传路由上配成 [`MAX_CHUNK`],与 tower 的层同值)先断流,或这里的
/// 累计上限后到——都归成同一句 400,调用侧看到的说法只有一种。
fn multipart_failed(e: MultipartError) -> Response {
    if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
        bad(&format!("插件包超过 {} MiB 的上限", MAX_PLUGIN / 1024 / 1024))
    } else {
        bad(&format!("上传的 multipart 请求解析失败：{e}"))
    }
}

/// 解包、校验并写库,`upload_plugin` 的同步主体。每一步失败都带着可操作的
/// 原因返回(它就是 400 的响应体)。
fn install_plugin(app: &App, archive: &[u8]) -> Result<Value, anyhow::Error> {
    use anyhow::{bail, Context};

    let files = unpack_plugin(archive)?;
    let toml_text = files.get("plugin.toml").context("插件包里没有 plugin.toml")?;
    if toml_text.len() > PLUGIN_MANIFEST_MAX {
        bail!("plugin.toml 超过 64 KiB");
    }
    let toml_text = String::from_utf8(toml_text.clone()).context("plugin.toml 不是合法的 UTF-8 文本")?;
    let manifest = Manifest::parse(&toml_text)?;
    let wasm =
        files.get(&manifest.wasm_entry).with_context(|| format!("插件包里没有 {}", manifest.wasm_entry))?;
    if wasm.len() > PLUGIN_WASM_MAX {
        bail!("{} 超过 {} MiB 的上限", manifest.wasm_entry, PLUGIN_WASM_MAX >> 20);
    }
    let wasm_sha256 = hex::encode(Sha256::digest(wasm));

    // 预热校验:用一条临时行(id=0,不落库)走真实的加载路径,把「manifest 写
    // 错了」「模块缺导出」「模块编译不过」在上传时就暴露。失败不拒绝入库:
    // status 保持 disabled、原因写进 last_error,面板上点开就能看到。
    let candidate = PluginRow {
        id: 0,
        plugin_id: manifest.plugin_id.clone(),
        name: manifest.name.clone(),
        version: manifest.version.clone(),
        manifest_json: toml_text.clone(),
        wasm_blob: wasm.clone(),
        wasm_sha256: String::new(),
        enabled: false,
        status: "disabled".into(),
        last_error: None,
        uploaded_at: 0,
    };
    let last_error = plugin::load(&app.engine, &candidate).err().map(|e| format!("{e:#}"));

    // 同 plugin_id 的上传按版本判断：更高才替换（保行 id、kv 与 plugin_data），
    // 不高则报错。首次安装与升级在这里没有分岔，由 db 那侧一并判定。
    let (row, replaced) = app.db.install_plugin_package(
        &manifest.plugin_id,
        &manifest.name,
        &manifest.version,
        &toml_text,
        wasm,
        &wasm_sha256,
    )?;
    if replaced {
        // 换掉的可能是正跑在内存里的那一份：旧实例必须当场下线，否则面板写着
        // 「已停用」而旧模块还在收 tick 与事件。新包等操作员重新启用才装载。
        app.plugins.write().unwrap_or_else(|e| e.into_inner()).disable_plugin(row.id);
    }
    if let Some(error) = &last_error {
        app.db.set_plugin_status(row.id, "disabled", Some(error))?;
    }
    Ok(json!({
        "id": row.id,
        "plugin_id": row.plugin_id,
        "version": manifest.version,
        "status": "disabled",
        "last_error": last_error,
        "replaced": replaced,
    }))
}

/// 把 tar.gz 的内容解进一个 `文件名 -> 字节` 的表。不落盘:上限之内整个包
/// 都在内存里,而 8 MiB 的入站上限已经把这里能见到的东西封住了。文件名取
/// 路径的最后一段,`./plugin.toml` 与带一层目录的包都能取到。
fn unpack_plugin(archive: &[u8]) -> Result<HashMap<String, Vec<u8>>, anyhow::Error> {
    use anyhow::{bail, Context};
    use std::io::Read;
    use std::path::{Component, Path};

    let mut tar = tar::Archive::new(flate2::read::GzDecoder::new(archive));
    let mut files = HashMap::new();
    let mut expanded = 0u64;
    for (seen, entry) in tar.entries()?.enumerate() {
        let mut entry = entry.context("插件包不是有效的 tar.gz")?;
        if seen >= PLUGIN_MAX_ENTRIES {
            bail!("插件包里的条目超过 {} 个", PLUGIN_MAX_ENTRIES);
        }
        // 路径先于内容:绝对路径与 `..` 在这里就该被拒,后面的上限检查才有
        // 一个可信的路径可报。`./` 前缀是 tar 的常态,不算越界。
        let path = entry.path()?.to_path_buf();
        for component in Path::new(&path).components() {
            match component {
                Component::ParentDir => bail!("插件包里的路径包含 `..`：{}", path.display()),
                Component::RootDir | Component::Prefix(_) => {
                    bail!("插件包里的路径是绝对路径：{}", path.display())
                }
                _ => {}
            }
        }
        let kind = entry.header().entry_type();
        if kind.is_dir() {
            continue; // 目录条目不携带数据,跳过而不是拒绝:打包工具常带它们。
        }
        if !kind.is_file() {
            bail!("插件包里有不支持的条目（仅接受普通文件）：{}", path.display());
        }
        let size = entry.size();
        if size > PLUGIN_MAX_FILE {
            bail!("{} 超过单个文件 {} MiB 的上限", path.display(), PLUGIN_MAX_FILE >> 20);
        }
        // 减法而不是加法:两个上限值相加会溢出,而 size 已知是较小的一方。
        if expanded > PLUGIN_MAX_EXPANDED - size {
            bail!("插件包解压后超过 {} MiB", PLUGIN_MAX_EXPANDED >> 20);
        }
        expanded += size;
        let mut data = Vec::with_capacity(size as usize);
        entry.read_to_end(&mut data).with_context(|| format!("读取 {} 失败", path.display()))?;
        let name =
            path.file_name().and_then(|n| n.to_str()).context("插件包里的文件名不是合法的 UTF-8")?.to_owned();
        files.insert(name, data);
    }
    if files.is_empty() {
        bail!("插件包里没有任何文件");
    }
    Ok(files)
}

/// 面板的插件列表(R12)。manifest_json 就在行里,把 subscribes 与 v2 的能力
/// 声明(page/tick/cleanup)解出来一起返回,前端画事件徽标与决定是否显示
/// 「页面」「清理」入口不必再猜。
pub async fn list_plugins(_: Admin, State(app): State<Shared>) -> Response {
    match app.db.plugin_summaries() {
        Ok(rows) => Json(
            rows.into_iter()
                .map(|r| {
                    // manifest 上传时已通过校验;这里容错而不是失败,一行坏
                    // manifest(手工改库)不该让整个列表 500。
                    let m = Manifest::parse(&r.manifest_json).ok();
                    json!({
                        "id": r.id,
                        "plugin_id": r.plugin_id,
                        "name": r.name,
                        "version": r.version,
                        "enabled": r.enabled,
                        "status": r.status,
                        "last_error": r.last_error,
                        "uploaded_at": r.uploaded_at,
                        "subscribes": m.as_ref().map(|m| m.subscribes.clone()).unwrap_or_default(),
                        "page": m.as_ref().and_then(|m| m.page.as_ref()).map(|p| p.title.clone()),
                        "tick": m.as_ref().map(|m| m.tick).unwrap_or(false),
                        "cleanup": m.as_ref().map(|m| m.cleanup).unwrap_or(false),
                        // 声明的渠道配置字段:面板「配置」对话框据此渲染标签、
                        // 标注必填、显示提示,不必让操作者猜 key 名。`type` 决定
                        // 单行还是多行控件,`default` 是没配值时预填的文案——两者
                        // 都是通用能力,面板不知道哪个字段是「模板」。
                        "config": m.as_ref().map(|m| m.kv.iter().map(|c| json!({
                            "key": c.key.clone(),
                            "label": c.label.clone(),
                            "required": c.required,
                            "hint": c.hint.clone(),
                            "type": c.kind.clone(),
                            "default": c.default.clone(),
                        })).collect::<Vec<_>>()).unwrap_or_default(),
                    })
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(e) => fail(e),
    }
}

/// 删除插件,连同它的 kv 行。db 行、setting 行、内存里的已加载实例三处一起
/// 收:任何一处留下都会以别的方式回来——行留着列表里就还有它,kv 留着删除
/// 再重传同名插件会捡到旧的渠道配置,内存留着它还会继续收事件。
pub async fn delete_plugin(_: Admin, State(app): State<Shared>, Path(id): Path<i64>) -> Response {
    let plugin_id = match plugin_or_404(&app, id) {
        Ok(plugin_id) => plugin_id,
        Err(resp) => return resp,
    };
    // 行与 kv 在一条事务里删:两次独立调用之间失败的话,重试会撞上
    // plugin_or_404 的 404,kv 孤儿永久留在 setting 表里。
    match app.db.delete_plugin_with_kv(id, &plugin_id) {
        // db 行与 kv 都删净之后才动内存:失败路径上插件保持原状,重试即是。
        Ok(()) => {
            app.plugins.write().unwrap_or_else(|e| e.into_inner()).remove_plugin(id);
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => fail(e),
    }
}

/// 启用插件:先写 db 的开关,再装进内存。加载失败时回滚开关再返回 400:
/// Registry 已把 `failed` 与原因落库,但 `enabled` 还是上面写成的 1,留着
/// 它,面板列表里就是"已启用"与 failed 的矛盾状态,重启后的预加载还会把
/// 同一个失败再跑一遍。作者改完包重新上传即可。
pub async fn enable_plugin(_: Admin, State(app): State<Shared>, Path(id): Path<i64>) -> Response {
    if let Err(resp) = plugin_or_404(&app, id) {
        return resp;
    }
    if let Err(e) = app.db.set_plugin_enabled(id, true) {
        return fail(e);
    }
    // 写锁在派发 tick 之前必须收掉:插件执行会取同一把读锁,guard 若活到 match
    // 结束,这次 tick 就是 dispatch_ticks 文档里写的那种自死锁。
    let loaded = {
        let mut registry = app.plugins.write().unwrap_or_else(|e| e.into_inner());
        registry.enable_plugin(&app, id)
    };
    match loaded {
        Ok(()) => {
            // 启用即刻对它跑一次 tick:插件禁用期间宿主发生的变更不会派发给它,
            // 只能靠这次(以及它自己的定时 tick)对齐。wasm 是 CPU 活,挪进
            // blocking 线程;不 await——插件 tick 里可能有网络往返,最长会占满
            // 钩子超时,不该拖住这个响应。句柄仍要跟一下:调度失败或被取消时,
            // 面板那次「启用」看起来一切正常,而这次对齐根本没跑过。
            let app = app.clone();
            let tick = tokio::task::spawn_blocking(move || plugin::Registry::dispatch_tick_one(&app, id));
            tokio::spawn(async move {
                if let Err(e) = tick.await {
                    warn!(plugin = id, "启用即 tick 没有跑完: {e}");
                }
            });
            Json(json!({"ok": true})).into_response()
        }
        Err(e) => {
            let message = format!("{e:#}");
            // 回滚开关并保住 failed 状态:`set_plugin_enabled(false)` 会把
            // status 覆盖回 disabled、清掉 last_error,随后这条把 Registry
            // 已写的 failed 与原因再放回去。
            if let Err(e) = app
                .db
                .set_plugin_enabled(id, false)
                .and_then(|()| app.db.set_plugin_status(id, "failed", Some(&message)))
            {
                return fail(e);
            }
            bad(&format!("插件加载失败，已标记为 failed：{message}"))
        }
    }
}

/// 禁用插件:与启用对称,只是加载不可能失败,没有错误分支可言。
pub async fn disable_plugin(_: Admin, State(app): State<Shared>, Path(id): Path<i64>) -> Response {
    if let Err(resp) = plugin_or_404(&app, id) {
        return resp;
    }
    if let Err(e) = app.db.set_plugin_enabled(id, false) {
        return fail(e);
    }
    app.plugins.write().unwrap_or_else(|e| e.into_inner()).disable_plugin(id);
    Json(json!({"ok": true})).into_response()
}

/// manifest 声明的必填配置里,面板还没填的那些。返回给人看的名字:有 label
/// 就 `标签(key)`,没有就 `key`。
///
/// 「没填」= 行不存在**或值为空**:`host_kv_get` 对两者都返回 0(README
/// 「`0 = 无值或空`」),这两类必须与插件运行时看到的一致,否则会出现「预检说填好
/// 了、插件说没有」这种最难查的分歧。纯空白比插件看到的更严——面板不会拦,操作员
/// 多半是手滑;与其让插件拿着一个空格去请求 Telegram,不如在这里点出来。
///
/// 读库失败走 `Err` 而不是被算成「没填」:那是库故障,调用方要报 500,不能把故障
/// 说成一句自信的「你还没配」。
fn missing_required_config(app: &App, plugin_id: &str, manifest: &Manifest) -> anyhow::Result<Vec<String>> {
    let mut missing = Vec::new();
    for decl in manifest.kv.iter().filter(|d| d.required) {
        let stored = app.db.try_get(&format!("plugin.{plugin_id}:{}", decl.key))?;
        if stored.is_none_or(|v| v.trim().is_empty()) {
            missing.push(match decl.label.as_deref() {
                Some(label) if !label.trim().is_empty() => format!("{label}({})", decl.key),
                _ => decl.key.clone(),
            });
        }
    }
    Ok(missing)
}

/// 一次「测试」要派发的合成事件:宿主自身事件用真实结构(字段齐全),`plugin_`
/// 前缀的事件宿主一无所知,只能用插件在 manifest 里声明的样例回放(KTD4)。
///
/// 按 `subscribes` 的顺序返回 `(事件名, Option<事件>)`;`None` 表示这条测不了
/// ——没声明样例,编一个空载荷派发过去只会让插件报解析失败,操作员会以为是自己
/// 的插件坏了。
///
/// 合成事件一律用 `node_id: 0`:真实节点 id 为正,订阅节点事件的插件据此忽略它,
/// 一次测试就不会在别人的数据里留下残留。
fn synthetic_events(manifest: &Manifest, now: i64) -> Vec<(String, Option<Event>)> {
    manifest
        .subscribes
        .iter()
        .map(|name| {
            let event = match name.as_str() {
                // 静默时长留一段:0 秒的「已离线」测不出文案的样子。
                Event::AGENT_OFFLINE => Some(Event::AgentOffline {
                    node_id: 0,
                    name: "test".into(),
                    observed_at: now,
                    last_seen_at: now - 300,
                }),
                Event::AGENT_ONLINE => {
                    Some(Event::AgentOnline { node_id: 0, name: "test".into(), observed_at: now })
                }
                // 新增与删除共用同一个 created_at:订阅节点事件的插件按「身份相符」
                // 判断要不要真删,这一配对净效果为零,测试不会留下一条假记录。
                Event::NODE_ADDED => {
                    Some(Event::NodeAdded { node_id: 0, name: "test".into(), created_at: now })
                }
                Event::NODE_DELETED => {
                    Some(Event::NodeDeleted { node_id: 0, name: "test".into(), created_at: now })
                }
                plugin_event => manifest
                    .samples
                    .iter()
                    .find(|sample| sample.name == plugin_event)
                    .and_then(|sample| serde_json::from_str(&sample.payload).ok())
                    .map(|payload| Event::Plugin { name: plugin_event.to_owned(), payload }),
            };
            (name.clone(), event)
        })
        .collect()
}

/// 测试通知(R12):按插件声明的订阅**逐条**合成事件并派发,每条都走与真实派发
/// 完全相同的执行路径(超时、fuel、宿主函数),但绕过 emit 与 notification_log
/// ——一次手工测试不占幂等键,真实事件的成功与否不该被它覆盖(U4 的 dispatch_one)。
/// 一次点击把每个模板都真发一遍,所以每条的结果都要回给面板。
///
/// 派发前先按 manifest 的 `[[kv]]` 预检必填项:插件返回 `other:2` 这种码,
/// 操作者从面板上看不出缺的是什么。只有这一条路径做预检——真实派发没有 400 可
/// 给,后台事件旁边也没有操作员。预检拦下的是整批,不是第一条。
pub async fn test_plugin(_: Admin, State(app): State<Shared>, Path(id): Path<i64>) -> Response {
    let plugin_id = match plugin_or_404(&app, id) {
        Ok(plugin_id) => plugin_id,
        Err(resp) => return resp,
    };
    // 未启用/加载失败时取不到 manifest,枚举不出订阅也就不可能合成任何一条:
    // 直接说清状况,而不是报「测试了 0 条」。
    let Some(manifest) = app.plugins.read().unwrap_or_else(|e| e.into_inner()).manifest_of(id) else {
        return bad("插件未启用或加载失败；先启用它再测试");
    };
    let missing = match missing_required_config(&app, &plugin_id, &manifest) {
        Ok(missing) => missing,
        Err(e) => return fail(e),
    };
    if !missing.is_empty() {
        return bad(&format!("插件缺少必填配置:{}；请在插件的「配置」里填写后再测试", missing.join("、")));
    }

    let handle = tokio::runtime::Handle::current();
    let mut results = Vec::new();
    for (name, event) in synthetic_events(&manifest, Utc::now().timestamp()) {
        let Some(event) = event else {
            results.push(json!({
                "event": name,
                "result": "no_sample",
                "elapsed_ms": 0,
                "detail": "插件没有为这个事件声明 [[sample]] 样例载荷，无法测试",
            }));
            continue;
        };
        // `dispatch_one` 自己只在取插件快照时借一次读锁,执行期间不持锁,所以
        // 这里不必再套一层 guard。wasm 是 CPU 活,仍挪进 blocking 线程,用预先
        // 取好的 runtime handle 驱动——run_one 的 spawn 与超时照常落在 runtime 上。
        let app = app.clone();
        let handle = handle.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            handle.block_on(plugin::Registry::dispatch_one(&app, id, &event))
        })
        .await;
        match outcome {
            Ok(Ok(entry)) => results.push(json!({
                "event": name,
                "result": entry.result,
                "elapsed_ms": entry.elapsed_ms,
                // 插件自己打的话:错误码是它私有的,`other:2` 光看数字排不了障。
                "detail": entry.detail,
            })),
            // 预检之后才被停用这种竞态：把这条记成失败，而不是把整批结果丢掉——
            // 前面几条可能已经真的发出去了(给 Telegram 发了通知),操作员必须
            // 看到哪一条发了、哪一条没发。
            Ok(Err(e)) => results.push(json!({
                "event": name,
                "result": "not_loaded",
                "elapsed_ms": 0,
                "detail": format!("插件未启用或加载失败,这一条没能派发:{e:#}"),
            })),
            Err(e) => return fail(anyhow::anyhow!(e)),
        }
    }
    Json(json!({"plugin_id": plugin_id, "results": results})).into_response()
}

/// 派发日志的分页查询参数(R16)。`page` 从 1 起算,`page_size` 是单页条数;
/// 都缺省(page=1、page_size=50)。越界值就近夹取而不是 400:这是操作员自己的
/// 面板,夹到合法区间比报错更省一次往返。
#[derive(Deserialize, Default)]
pub struct LogPage {
    page: Option<u32>,
    page_size: Option<u32>,
}

/// 单页条数的缺省与上限。缓冲是全局的(`DISPATCH_LOG_CAP`),单个插件最多可
/// 占满全部 1000 条;上限 500 只限制单次序列化的规模,一页装不下就翻页。
const LOG_PAGE_SIZE_DEFAULT: u32 = 50;
const LOG_PAGE_SIZE_MAX: u32 = 500;

/// 一个插件的派发日志(R16):内存环形缓冲的快照按 plugin_id 过滤,分页返回。
/// 缓冲是进程内的,重启后为空——面板把它当「刚才发生了什么」看,长期审计在
/// notification_log。快照新 → 旧,分页延续同一顺序:第一页是最新的一段,
/// total 是该插件在缓冲里的全部条数,翻页不改变它。
pub async fn plugin_dispatch_log(
    _: Admin,
    State(app): State<Shared>,
    Path(id): Path<i64>,
    Query(page): Query<LogPage>,
) -> Response {
    let plugin_id = match plugin_or_404(&app, id) {
        Ok(plugin_id) => plugin_id,
        Err(resp) => return resp,
    };
    let page_no = page.page.unwrap_or(1).max(1);
    let page_size = page.page_size.unwrap_or(LOG_PAGE_SIZE_DEFAULT).clamp(1, LOG_PAGE_SIZE_MAX);
    let filtered: Vec<_> = app
        .plugins
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .dispatch_log_snapshot()
        .into_iter()
        .filter(|entry| entry.plugin_id == plugin_id)
        .collect();
    // (page-1)*page_size 在 u32 相乘会溢出,先升位再算;跳过的是快照下标,
    // 总量至多环形缓冲的 1000,usize 装得下。
    let skip = (page_no as u64 - 1) * page_size as u64;
    let entries: Vec<_> = filtered.iter().skip(skip as usize).take(page_size as usize).collect();
    Json(json!({
        "entries": entries,
        "total": filtered.len(),
        "page": page_no,
        "page_size": page_size,
    }))
    .into_response()
}

/// 一个声明了 page 的启用插件渲染它的面板页面(U5/KTD5)。宿主调插件的
/// `render_page` 导出,把返回的 JSON UI 描述原样转给前端;插件侧失败
/// (超时/trap/非 2xx)返回 502 由前端显示错误卡片。
pub async fn render_plugin_page(_: Admin, State(app): State<Shared>, Path(id): Path<i64>) -> Response {
    let plugin_id = match plugin_or_404(&app, id) {
        Ok(plugin_id) => plugin_id,
        Err(resp) => return resp,
    };
    let manifest = match loaded_manifest(&app, id) {
        Ok(m) => m,
        Err(resp) => return resp,
    };
    if manifest.page.is_none() {
        return not_found("该插件没有声明页面");
    }
    let outcome = call_plugin_json(app.clone(), id, "render_page", b"{}").await;
    match outcome {
        Ok(bytes) if bytes.is_empty() => Json(json!({})).into_response(),
        Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
            Ok(v) => Json(v).into_response(),
            Err(e) => fail(anyhow::anyhow!("插件页面描述不是合法 JSON: {e}")),
        },
        Err(e) => {
            warn!(plugin = %plugin_id, "render_page 失败: {e:#}");
            (StatusCode::BAD_GATEWAY, "插件页面渲染失败").into_response()
        }
    }
}

/// 把一次页面交互交给插件处理(U5):body 是 `{action, ...}` 的 JSON,宿主调
/// 插件的 `on_action`,返回插件给出的响应(新页面描述或成功提示)。
pub async fn plugin_page_action(
    _: Admin,
    State(app): State<Shared>,
    Path(id): Path<i64>,
    body: axum::body::Bytes,
) -> Response {
    let plugin_id = match plugin_or_404(&app, id) {
        Ok(plugin_id) => plugin_id,
        Err(resp) => return resp,
    };
    let manifest = match loaded_manifest(&app, id) {
        Ok(m) => m,
        Err(resp) => return resp,
    };
    if manifest.page.is_none() {
        return not_found("该插件没有声明页面");
    }
    if body.len() > ACTION_BODY_MAX {
        return bad("action 请求体超过上限");
    }
    let outcome = call_plugin_json(app.clone(), id, "on_action", &body).await;
    match outcome {
        Ok(bytes) if bytes.is_empty() => Json(json!({})).into_response(),
        Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
            Ok(v) => Json(v).into_response(),
            Err(e) => fail(anyhow::anyhow!("插件 action 响应不是合法 JSON: {e}")),
        },
        Err(e) => {
            warn!(plugin = %plugin_id, "on_action 失败: {e:#}");
            (StatusCode::BAD_GATEWAY, "插件处理失败").into_response()
        }
    }
}

/// 插件自己的数据清理入口(U9/KTD11):宿主只转发调用并回传结果,不碰插件
/// 数据语义。只有声明 cleanup 的插件可用,否则 404。
pub async fn plugin_cleanup(_: Admin, State(app): State<Shared>, Path(id): Path<i64>) -> Response {
    let plugin_id = match plugin_or_404(&app, id) {
        Ok(plugin_id) => plugin_id,
        Err(resp) => return resp,
    };
    let manifest = match loaded_manifest(&app, id) {
        Ok(m) => m,
        Err(resp) => return resp,
    };
    if !manifest.cleanup {
        return not_found("该插件没有声明清理能力");
    }
    let outcome = call_plugin_json(app.clone(), id, "on_cleanup", b"{}").await;
    match outcome {
        Ok(bytes) if bytes.is_empty() => Json(json!({"freed_bytes": 0, "pruned": 0})).into_response(),
        Ok(bytes) => match serde_json::from_slice::<Value>(&bytes) {
            Ok(v) => Json(v).into_response(),
            Err(e) => fail(anyhow::anyhow!("清理响应不是合法 JSON: {e}")),
        },
        Err(e) => {
            warn!(plugin = %plugin_id, "on_cleanup 失败: {e:#}");
            (StatusCode::BAD_GATEWAY, "插件清理失败").into_response()
        }
    }
}

/// 调一个插件的 JSON 入/出导出。`Registry::call_json` 走的是与派发同一套
/// fuel/超时隔离,并且自己只在取插件快照时借一次读锁——插件执行期间不持锁,
/// 插件在 `on_action` 里发事件不会重入这把锁。wasm 是 CPU 活,仍挪进 blocking
/// 线程;这里不再套外层 `block_on`,嵌套 `block_on` 会让宿主里的同名调用
/// panic。
async fn call_plugin_json(app: Shared, id: i64, hook: &str, input: &[u8]) -> Result<Vec<u8>, anyhow::Error> {
    let hook = hook.to_owned();
    let input = input.to_vec();
    tokio::task::spawn_blocking(move || plugin::Registry::call_json(&app, id, &hook, &input))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?
}

/// 页面 action 请求体的上限,与该路由所在的 64 KiB 层一致。
const ACTION_BODY_MAX: usize = 64 * 1024;

/// kv 的 key 校验,set 与 delete 共用:set 侧挡住不能落库的形状,delete
/// 侧对同样的形状按 400 拒绝而不是当成不存在的行吞掉——它们只能是打错的
/// 路由参数,报错比静默成功更接近调用方的预期。
///
/// 形状规则单点在 [`plugin::kv_key_problem`],与 manifest 的 `[[kv]]` 声明用的是
/// 同一份;这里只把它折成面板的文案。首尾空白也拒:接口写入的是**原样** key,
/// 带空白的那一行与插件声明的字段永远对不上。
fn kv_key_error(key: &str) -> Option<Response> {
    match plugin::kv_key_problem(key)? {
        plugin::KvKeyProblem::Empty => Some(bad("key 不能为空")),
        plugin::KvKeyProblem::Padded => {
            Some(bad("key 首尾不能有空白：写入的是原样 key，带空白的那一行与插件声明的字段对不上"))
        }
        plugin::KvKeyProblem::Colon => Some(bad("key 不能包含 ':'（它是 kv 命名空间的分隔符）")),
        plugin::KvKeyProblem::TooLong => Some(bad(&format!("key 超过 {} 字节的上限", plugin::KV_KEY_MAX))),
    }
}

/// 写一个插件的 kv 行(R13):渠道配置这类「面板替插件填」的值。落在与
/// host_kv_set 相同的 `plugin.<plugin_id>:<key>` 命名空间与相同的 8 KiB 上限
/// 里,插件读到的与作者填的是同一行。
pub async fn set_plugin_kv(
    _: Admin,
    State(app): State<Shared>,
    Path((id, key)): Path<(i64, String)>,
    Json(body): Json<Value>,
) -> Response {
    let plugin_id = match plugin_or_404(&app, id) {
        Ok(plugin_id) => plugin_id,
        Err(resp) => return resp,
    };
    if let Some(resp) = kv_key_error(&key) {
        return resp;
    }
    let Some(value) = body.get("value").and_then(Value::as_str) else {
        return bad("body 必须是 {\"value\": \"...\"} 形式的对象");
    };
    if value.len() > plugin::KV_VALUE_MAX {
        return bad(&format!(
            "value 超过 {} KiB 的上限（与插件的 host_kv_set 同限）",
            plugin::KV_VALUE_MAX / 1024
        ));
    }
    match app.db.set(&format!("plugin.{plugin_id}:{key}"), value) {
        Ok(()) => Json(json!({"ok": true})).into_response(),
        Err(e) => fail(e),
    }
}

/// 列出一个插件的全部 kv 行(R13),key 去掉命名空间前缀,面板照原样回填表单。
pub async fn list_plugin_kv(_: Admin, State(app): State<Shared>, Path(id): Path<i64>) -> Response {
    let plugin_id = match plugin_or_404(&app, id) {
        Ok(plugin_id) => plugin_id,
        Err(resp) => return resp,
    };
    match app.db.plugin_kv(&plugin_id) {
        Ok(pairs) => Json(
            pairs.into_iter().map(|(key, value)| json!({"key": key, "value": value})).collect::<Vec<_>>(),
        )
        .into_response(),
        Err(e) => fail(e),
    }
}

/// 删除一个插件的一个 kv 行(R13),与 set 同名路由、同一 key 校验。键名是
/// 拼好的精确键,不走 LIKE:调用方给的是 key 而不是模式,`%` 与 `_` 只能按
/// 字符匹配。删除不存在的行不是错误——两处面板同时打开,后点的那个同样
/// 达成目标(与 `delete_session` 对同一竞态的处理一致)。
pub async fn delete_plugin_kv_route(
    _: Admin,
    State(app): State<Shared>,
    Path((id, key)): Path<(i64, String)>,
) -> Response {
    let plugin_id = match plugin_or_404(&app, id) {
        Ok(plugin_id) => plugin_id,
        Err(resp) => return resp,
    };
    if let Some(resp) = kv_key_error(&key) {
        return resp;
    }
    match app.db.delete_setting(&format!("plugin.{plugin_id}:{key}")) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => fail(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 仍留在 api.rs 的测试依赖:分层 router 里挂主 router 的 `nodes`、restore
    // 测试直接调用的 `db_restore`/`Chunk`,以及两道 body limit 共用的 `MAX_CHUNK`。
    use crate::api::{db_restore, nodes, Chunk, MAX_CHUNK};
    // 与 api.rs 的测试模块同源的会话/建库助手。
    use crate::auth::{random_token, sha256};
    use crate::db::Db;
    use axum::extract::Query;
    use axum::http::{header, HeaderMap};

    // ---- plugins(U5) ----
    //
    // 上传走 router 级整调(oneshot),分层照抄 main.rs:POST /api/plugins 在
    // 8 MiB 的 merge 子 router 里,主 router 的 64 KiB 层在它之外。Multipart
    // 提取器还会在 tower 的层之上再套一层自己的 body limit(缺省 2 MiB),main
    // 用 DefaultBodyLimit 配平了它——这里照抄,否则 2 MiB 以上的包在测试里就
    // 先失败,而生产里也会(这是本分层测试真正抓过的 bug)。

    use tower::ServiceExt as _;

    // 与 plugin.rs 共享的最小合法模块:加载、启停、测试与日志全都用它。
    use crate::plugin::{KV_TICK_WAT, MINIMAL_WAT};

    fn plugin_manifest(plugin_id: &str, abi_version: i64) -> String {
        plugin_manifest_at(plugin_id, abi_version, "1.0.0")
    }

    /// 同上，但指定版本：升级路径的测试要造出更高/更低的版本。
    fn plugin_manifest_at(plugin_id: &str, abi_version: i64, version: &str) -> String {
        format!(
            "plugin_id = \"{plugin_id}\"\nname = \"Test Plugin\"\nversion = \"{version}\"\n\
             abi_version = {abi_version}\nsubscribes = [\"agent_offline\", \"plugin_expiry_soon\"]\n"
        )
    }

    /// 内存里打一个 tar.gz,entry 名与字节由调用方给。checksum 由
    /// `append_data` 自己补齐,这里不重复设。
    fn tarball(files: &[(&str, Vec<u8>)]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        {
            let mut builder = tar::Builder::new(&mut encoder);
            for (name, bytes) in files {
                let mut header = tar::Header::new_gnu();
                header.set_size(bytes.len() as u64);
                header.set_mode(0o644);
                builder.append_data(&mut header, name, bytes.as_slice()).unwrap();
            }
            builder.into_inner().unwrap();
        }
        encoder.finish().unwrap()
    }

    /// 重算 GNU 头的 checksum:checksum 字段在 148..156,计算时按空格。
    /// 直接篡改原始 tar 字节的 fixture 在改完感兴趣的字段后调用。
    fn rechecksum(raw: &mut [u8]) {
        for b in &mut raw[148..156] {
            *b = b' ';
        }
        let sum: u32 = raw[..512].iter().map(|&b| b as u32).sum();
        raw[148..156].copy_from_slice(format!("{:06o}\0 ", sum).as_bytes());
    }

    fn gzipped(raw: &[u8]) -> Vec<u8> {
        use std::io::Write as _;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(raw).unwrap();
        encoder.finish().unwrap()
    }

    /// 打一个 entry 名任意的 tar.gz:tar::Builder 拒绝写 `..` 与绝对路径,而要
    /// 防的正是绕过了 Builder 的包——把名字直接改在原始 tar 字节上(重算
    /// checksum)再压缩。
    fn tarball_with_entry_name(name: &str, bytes: &[u8]) -> Vec<u8> {
        assert!(name.len() < 100, "tar 的 name 字段只有 100 字节");
        let mut raw = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut raw);
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o644);
            builder.append_data(&mut header, "placeholder", bytes).unwrap();
            builder.into_inner().unwrap();
        }
        // GNU 头:name 在 0..100。
        for (i, b) in raw[..100].iter_mut().enumerate() {
            *b = name.as_bytes().get(i).copied().unwrap_or(0);
        }
        rechecksum(&mut raw);
        gzipped(&raw)
    }

    /// 打一个头里声明的大小与实际内容脱钩的 tar.gz:单 entry 上限检查读的是
    /// 头里的声明值,「声明 16 MiB+1、内容为空」的包才能证明检查发生在读入
    /// 之前,而不是把 16 MiB 真的吃进内存之后。
    fn tarball_with_declared_entry_size(size: u64) -> Vec<u8> {
        let mut raw = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut raw);
            let mut header = tar::Header::new_gnu();
            header.set_size(0);
            header.set_mode(0o644);
            builder.append_data(&mut header, "plugin.toml", &b""[..]).unwrap();
            builder.into_inner().unwrap();
        }
        // GNU 头:size 在 124..136(11 位八进制 + NUL)。
        let field = format!("{:011o}\0", size);
        assert_eq!(field.len(), 12);
        raw[124..136].copy_from_slice(field.as_bytes());
        rechecksum(&mut raw);
        gzipped(&raw)
    }

    /// 打一个带 symlink entry 的 tar.gz。大小合法、名字合法,唯一的越界是
    /// 条目类型——解包防线必须在读内容之前就按类型拒绝它。
    fn tarball_with_symlink() -> Vec<u8> {
        let mut raw = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut raw);
            let mut header = tar::Header::new_gnu();
            header.set_size(0);
            header.set_mode(0o644);
            header.set_entry_type(tar::EntryType::Symlink);
            header.set_link_name("/etc/passwd").unwrap();
            builder.append_data(&mut header, "evil-link", std::io::empty()).unwrap();
            builder.into_inner().unwrap();
        }
        gzipped(&raw)
    }

    fn plugin_archive(manifest: &str) -> Vec<u8> {
        tarball(&[
            ("plugin.toml", manifest.as_bytes().to_vec()),
            ("plugin.wasm", wat::parse_str(MINIMAL_WAT).unwrap()),
        ])
    }

    /// 难压缩的字节:让 gzip 之后仍然超线,上限检查面对的是真实的体量。
    fn noise(len: usize) -> Vec<u8> {
        let mut state = 0x2545F4914F6CDD1Du64;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 33) as u8
            })
            .collect()
    }

    /// 一个带 Weak 回指的 App:dispatch_one 要 upgrade 回 App 读 fuel/timeout
    /// 的 setting,占位 Registry 里没有这个回指。顺带立一个会话——router 级
    /// 整调会跑 Admin 提取器,cookie 在 plugin_request 里带上。
    fn plugin_app() -> std::sync::Arc<App> {
        let app = std::sync::Arc::new(App::for_test(Db::open(":memory:").unwrap()));
        app.plugins.write().unwrap_or_else(|e| e.into_inner()).init(&app);
        app.db.create_session(&sha256("plugin-router-test"), Utc::now().timestamp() + 3_600).unwrap();
        app
    }

    /// 一个 multipart 请求,`plugin` 字段携带 tar.gz,cookie 过 Admin 提取器。
    fn plugin_request(archive: Vec<u8>) -> axum::extract::Request {
        let boundary = "monitor-plugin-test";
        let mut body = format!(
            "--{boundary}\r\n\
             content-disposition: form-data; name=\"plugin\"; filename=\"plugin.tar.gz\"\r\n\
             content-type: application/gzip\r\n\r\n"
        )
        .into_bytes();
        body.extend_from_slice(&archive);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        let len = body.len();
        axum::extract::Request::builder()
            .method("POST")
            .uri("/api/plugins")
            .header(header::CONTENT_TYPE, format!("multipart/form-data; boundary={boundary}"))
            .header(header::COOKIE, "monitor_session=plugin-router-test")
            // 浏览器发 FormData 总带 Content-Length;tower 的 body limit 层靠它
            // 在读第一个字节之前拒绝超限包。
            .header(header::CONTENT_LENGTH, len)
            .body(axum::body::Body::from(body))
            .unwrap()
    }

    /// 与 main.rs 相同的分层:上传路由挂在 8 MiB 的 merge 子 router,主 router
    /// 的 64 KiB 层在它之外。一个超过 64 KiB 的包从这里活着走到 handler,证明
    /// 挂载的层放行了大包(挂在主 router 的 64 KiB 层之下就会 413)。
    fn upload_router(app: &Shared) -> axum::Router {
        let uploads = axum::Router::new()
            .route("/api/plugins", axum::routing::post(upload_plugin))
            .layer(tower_http::limit::RequestBodyLimitLayer::new(MAX_CHUNK))
            .layer(axum::extract::DefaultBodyLimit::max(MAX_CHUNK))
            .with_state(app.clone());
        axum::Router::new()
            .route("/api/nodes", axum::routing::get(nodes))
            .layer(tower_http::limit::RequestBodyLimitLayer::new(64 * 1024))
            .merge(uploads)
            .with_state(app.clone())
    }

    async fn upload(app: &Shared, archive: Vec<u8>) -> Response {
        upload_router(app).oneshot(plugin_request(archive)).await.unwrap()
    }

    /// 同一条路由,但 body limit 抬到能装下超限包:handler 自己的累计上限是
    /// router 之外的第二道防线(生产里 tower 的层先断流),只有抬高第一道才
    /// 测得到它。
    async fn upload_past_router_limit(app: &Shared, archive: Vec<u8>) -> Response {
        let router = axum::Router::new()
            .route("/api/plugins", axum::routing::post(upload_plugin))
            .layer(axum::extract::DefaultBodyLimit::max(64 << 20))
            .with_state(app.clone());
        router.oneshot(plugin_request(archive)).await.unwrap()
    }

    async fn body_of(response: Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// 合法包上传即入库、默认不启用(KTD10),模块按字节原样保存,sha256 对得上,
    /// 列表带出 subscribes。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_uploaded_plugin_lands_disabled_in_the_table() {
        let app = plugin_app();
        let wasm = wat::parse_str(MINIMAL_WAT).unwrap();
        let archive = plugin_archive(&plugin_manifest("com.example.mailer", 2));

        assert_eq!(upload(&app, archive).await.status(), StatusCode::OK);
        let rows = app.db.list_plugins().unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!((row.plugin_id.as_str(), row.status.as_str()), ("com.example.mailer", "disabled"));
        assert!(!row.enabled, "上传后默认不启用");
        assert!(row.last_error.is_none(), "能加载的包不该带错误");
        assert_eq!(row.wasm_blob, wasm, "模块按字节原样保存");
        assert_eq!(row.wasm_sha256, hex::encode(Sha256::digest(&wasm)));
        assert!(!app.plugins.read().unwrap_or_else(|e| e.into_inner()).is_loaded(row.id));

        // 列表把 subscribes 从 manifest 解出来,前端画徽标不必再猜。
        let listed = body_of(list_plugins(Admin, State(app.clone())).await).await;
        assert_eq!(listed[0]["plugin_id"], "com.example.mailer");
        assert_eq!(listed[0]["subscribes"], json!(["agent_offline", "plugin_expiry_soon"]));
        assert_eq!(listed[0]["status"], "disabled");
        assert!(listed[0].get("wasm_blob").is_none(), "列表不携带模块字节");
    }

    /// 每一种坏包都带着原因被拒,并且什么都不写:manifest 缺失、ABI 不符、
    /// plugin_id 含 ':',路径带 `..` 或绝对路径的包(名字直接改在 tar 头上,
    /// 绕过打包工具的好心),以及解包防线的四种形态——条目数、单 entry 的
    /// 声明大小、manifest 体量、symlink 条目。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bad_packages_are_refused_with_a_reason_and_store_nothing() {
        let app = plugin_app();
        // 条目数超限:PLUGIN_MAX_ENTRIES + 1 个小文件,目录条目一样计入。
        let crowded: Vec<(String, Vec<u8>)> =
            (0..=PLUGIN_MAX_ENTRIES).map(|i| (format!("f{i}"), vec![b'x'])).collect();
        let crowded: Vec<(&str, Vec<u8>)> =
            crowded.iter().map(|(name, bytes)| (name.as_str(), bytes.clone())).collect();
        let cases: Vec<(Vec<u8>, &str)> = vec![
            // 没有 plugin.toml。
            (tarball(&[("plugin.wasm", wat::parse_str(MINIMAL_WAT).unwrap())]), "没有 plugin.toml"),
            // ABI 不符:v1 从此不受支持(KTD1)。
            (plugin_archive(&plugin_manifest("com.example.mailer", 1)), "abi_version"),
            // plugin_id 含 ':'(kv 命名空间的分隔符)。
            (plugin_archive(&plugin_manifest("com.example:mailer", 2)), "':'"),
            // 路径越出包外:`..` 与绝对路径。
            (
                tarball_with_entry_name(
                    "../plugin.toml",
                    plugin_manifest("com.example.mailer", 2).as_bytes(),
                ),
                "..",
            ),
            (
                tarball_with_entry_name(
                    "/etc/plugin.toml",
                    plugin_manifest("com.example.mailer", 2).as_bytes(),
                ),
                "绝对路径",
            ),
            // 条目数超限。
            (tarball(&crowded), "条目超过"),
            // 单 entry 声明的大小越过 16 MiB:检查必须发生在读入之前。
            (tarball_with_declared_entry_size(PLUGIN_MAX_FILE + 1), "单个文件"),
            // manifest 体量越过 64 KiB:内容不必是合法 TOML,长度检查在解析之前。
            (tarball(&[("plugin.toml", vec![b'#'; PLUGIN_MANIFEST_MAX + 1])]), "64 KiB"),
            // symlink 条目:不是普通文件,读内容之前就该被拒。
            (tarball_with_symlink(), "仅接受普通文件"),
        ];
        for (archive, needle) in cases {
            let refused = upload(&app, archive).await;
            assert_eq!(refused.status(), StatusCode::BAD_REQUEST, "{needle}");
            let bytes = axum::body::to_bytes(refused.into_body(), usize::MAX).await.unwrap();
            let text = String::from_utf8_lossy(&bytes);
            assert!(text.contains(needle), "响应应说明 `{needle}`,实际:{text}");
        }
        assert!(app.db.list_plugins().unwrap().is_empty(), "被拒的包一行都不写");
    }

    /// 同 plugin_id 的上传按版本走：**更高**版本就地替换——行 id 不变，kv 与
    /// plugin_data 原样保留，跑在内存里的旧实例当场下线并回到停用；版本没提高
    /// 则 400，已装的那份一行都不动。面板上的删除会连数据一起删，所以升级不能
    /// 靠「删了再传」。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_higher_version_replaces_in_place_and_keeps_the_plugin_data() {
        let app = plugin_app();
        let plugin_id = "com.example.a";
        let installed = upload(&app, plugin_archive(&plugin_manifest_at(plugin_id, 2, "1.0.0"))).await;
        assert_eq!(installed.status(), StatusCode::OK);
        assert_eq!(
            body_of(installed).await,
            json!({"id": 1, "plugin_id": plugin_id, "version": "1.0.0", "status": "disabled",
                    "last_error": null, "replaced": false}),
            "首次安装：replaced=false"
        );
        let row = app.db.list_plugins().unwrap().remove(0);

        // 启用它，再写入 kv 与插件数据——替换必须把这两样都留着。
        assert_eq!(enable_plugin(Admin, State(app.clone()), Path(row.id)).await.status(), StatusCode::OK);
        app.db.plugin_data_put(plugin_id, "node:1", "42").unwrap();
        app.db.set(&format!("plugin.{plugin_id}:bot_token"), "secret").unwrap();

        // 版本没提高：拒绝，且已装的那份不动。
        for stale in ["1.0.0", "0.9"] {
            let refused = upload(&app, plugin_archive(&plugin_manifest_at(plugin_id, 2, stale))).await;
            assert_eq!(refused.status(), StatusCode::BAD_REQUEST, "{stale}");
            let bytes = axum::body::to_bytes(refused.into_body(), usize::MAX).await.unwrap();
            let text = String::from_utf8_lossy(&bytes);
            assert!(text.contains("只有版本更高"), "响应该说清怎么升:{text}");
        }
        assert_eq!(app.db.list_plugins().unwrap()[0].version, "1.0.0", "被拒的上传不落库");

        // 版本更高：替换。
        let upgraded = upload(&app, plugin_archive(&plugin_manifest_at(plugin_id, 2, "2.0.0"))).await;
        assert_eq!(upgraded.status(), StatusCode::OK);
        let body = body_of(upgraded).await;
        assert_eq!((body["replaced"].as_bool(), body["version"].as_str()), (Some(true), Some("2.0.0")));

        let rows = app.db.list_plugins().unwrap();
        assert_eq!(rows.len(), 1, "替换不新增行");
        let after = &rows[0];
        assert_eq!((after.id, after.version.as_str()), (row.id, "2.0.0"), "行 id 不变，版本换了");
        assert!(!after.enabled && after.status == "disabled", "替换后回到停用");
        assert!(
            !app.plugins.read().unwrap_or_else(|e| e.into_inner()).is_loaded(row.id),
            "跑着的旧实例必须当场下线，否则面板写着停用而旧模块还在收事件"
        );
        assert_eq!(
            app.db.plugin_data_get(plugin_id, "node:1").unwrap().as_deref(),
            Some("42"),
            "插件数据留着"
        );
        assert_eq!(
            app.db.get(&format!("plugin.{plugin_id}:bot_token")).as_deref(),
            Some("secret"),
            "kv 留着"
        );
    }

    /// 编译不过的包也入库:status=disabled、原因在 last_error,作者在面板上
    /// 看到而不是从日志里找;对它启用得到的 400 一样带出原因。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_broken_wasm_lands_with_the_reason_and_cannot_be_enabled() {
        let app = plugin_app();
        let archive = tarball(&[
            ("plugin.toml", plugin_manifest("com.example.broken", 2).into_bytes()),
            ("plugin.wasm", b"\0asm\xde\xad\xbe\xef".to_vec()),
        ]);
        assert_eq!(upload(&app, archive).await.status(), StatusCode::OK);
        let row = &app.db.list_plugins().unwrap()[0];
        assert_eq!(row.status, "disabled");
        assert!(row.last_error.as_deref().unwrap().contains("编译失败"), "{:?}", row.last_error);

        assert_eq!(
            enable_plugin(Admin, State(app.clone()), Path(row.id)).await.status(),
            StatusCode::BAD_REQUEST
        );
        let row = app.db.get_plugin(row.id).unwrap().unwrap();
        assert_eq!(row.status, "failed", "失败的启用要落库成 failed");
        assert!(!row.enabled, "开关也要拨回去:留着 enabled=1,列表里就是「已启用」与 failed 并存的矛盾状态");
        assert!(!app.plugins.read().unwrap_or_else(|e| e.into_inner()).is_loaded(row.id));
    }

    /// 一个 3 MiB 的合法包(多塞一块难压缩的填充)经过照抄 main.rs 的分层
    /// router 完整入库。体量一次跨过两道线:64 KiB(主 router 的层——上传路由
    /// 必须挂在 8 MiB 的 merge 子 router 上,挂错层这条请求就 413)和 2 MiB
    /// (Multipart 提取器自己的缺省 body limit——main 用 DefaultBodyLimit 配平
    /// 了它,漏配的话包在解析阶段就失败)。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_multi_mib_legal_package_uploads_through_the_merged_router() {
        let app = plugin_app();
        let archive = tarball(&[
            ("plugin.toml", plugin_manifest("com.example.big", 2).into_bytes()),
            ("plugin.wasm", wat::parse_str(MINIMAL_WAT).unwrap()),
            ("assets/pad.bin", noise(3 * 1024 * 1024)),
        ]);
        assert!(archive.len() > 2 * 1024 * 1024, "fixture 必须跨过 2 MiB 的缺省 body limit");
        assert_eq!(upload(&app, archive).await.status(), StatusCode::OK);
        assert_eq!(app.db.list_plugins().unwrap().len(), 1);
    }

    /// 超过字节上限的包,两道防线各尽其职:router 的层先行断流(413),handler
    /// 的累计上限是它之外的第二道(400 带原因)——后者只有抬高前者才测得到。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_package_over_the_byte_cap_is_refused() {
        let app = plugin_app();
        let archive = tarball(&[
            ("plugin.toml", plugin_manifest("com.example.huge", 2).into_bytes()),
            ("plugin.wasm", noise(MAX_PLUGIN as usize + 1)), // 单 entry 仍在 16 MiB 内
        ]);
        assert!(archive.len() as u64 > MAX_PLUGIN);

        // 生产路径:8 MiB 的层先看到超限。
        assert_eq!(upload(&app, archive.clone()).await.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(app.db.list_plugins().unwrap().is_empty());

        // 抬高第一道之后,handler 自己的累计上限接住它。
        let refused = upload_past_router_limit(&app, archive).await;
        assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
        let bytes = axum::body::to_bytes(refused.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("上限"), "{}", String::from_utf8_lossy(&bytes));
        assert!(app.db.list_plugins().unwrap().is_empty());
    }

    /// 启停生命周期:enable 写库又装内存,test 走完整执行路径拿回结果,
    /// disable 两头都摘掉,test 随之变成 400。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn enable_test_and_disable_walk_the_full_lifecycle() {
        let app = plugin_app();
        assert_eq!(
            upload(&app, plugin_archive(&plugin_manifest("com.example.lifecycle", 2))).await.status(),
            StatusCode::OK
        );
        let id = app.db.list_plugins().unwrap()[0].id;

        assert_eq!(enable_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);
        let row = app.db.get_plugin(id).unwrap().unwrap();
        assert!(row.enabled && row.status == "enabled");
        assert!(app.plugins.read().unwrap_or_else(|e| e.into_inner()).is_loaded(id));

        // 测试通知:按订阅逐条合成、逐条真派发(plugin_manifest 订阅两条)。
        let tested = test_plugin(Admin, State(app.clone()), Path(id)).await;
        assert_eq!(tested.status(), StatusCode::OK);
        let body = body_of(tested).await;
        assert_eq!(body["plugin_id"], "com.example.lifecycle");
        let first = &body["results"][0];
        assert_eq!(first["event"], "agent_offline");
        assert_eq!(first["result"], "success");
        assert!(first["elapsed_ms"].as_u64().is_some());
        assert!(first["detail"].is_null(), "不打日志的插件 detail 就是 null");
        assert_eq!(body["results"].as_array().unwrap().len(), 2, "两条订阅各一条结果");

        assert_eq!(disable_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);
        let row = app.db.get_plugin(id).unwrap().unwrap();
        assert!(!row.enabled && row.status == "disabled");
        assert!(!app.plugins.read().unwrap_or_else(|e| e.into_inner()).is_loaded(id));
        assert_eq!(test_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::BAD_REQUEST);

        // 不存在的行号是 404,不是 500 或静默成功。
        assert_eq!(
            enable_plugin(Admin, State(app.clone()), Path(9999)).await.status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            disable_plugin(Admin, State(app.clone()), Path(9999)).await.status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(test_plugin(Admin, State(app.clone()), Path(9999)).await.status(), StatusCode::NOT_FOUND);
    }

    /// 声明 `tick` 的 manifest 加一个会写 kv 的 on_tick 模块。
    fn tick_archive(plugin_id: &str) -> Vec<u8> {
        let manifest = format!(
            "plugin_id = \"{plugin_id}\"\nname = \"Test Plugin\"\nversion = \"1.0.0\"\n\
             abi_version = 2\nsubscribes = []\ntick = true\n"
        );
        tarball(&[
            ("plugin.toml", manifest.into_bytes()),
            ("plugin.wasm", wat::parse_str(KV_TICK_WAT).unwrap()),
        ])
    }

    /// 启用即 tick 走的是 spawn_blocking 上的 fire-and-forget:轮询到它落账。
    async fn until_tick(app: &App) {
        for _ in 0..400 {
            if !app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch_log_snapshot().is_empty() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        panic!("启用即 tick 在 10 秒内没有落账");
    }

    /// 启用插件成功后宿主立刻跑一次它的 tick:插件停用期间发生的变更不会派发给
    /// 它(它在内存里根本不存在),只能靠这次和它自己的定时 tick 补上。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn enabling_a_plugin_runs_one_tick() {
        let app = plugin_app();
        assert_eq!(upload(&app, tick_archive("com.example.ticker")).await.status(), StatusCode::OK);
        let id = app.db.list_plugins().unwrap()[0].id;
        assert_eq!(app.db.get("plugin.com.example.ticker:called"), None, "启用之前不该跑过 tick");

        assert_eq!(enable_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);
        until_tick(&app).await;
        let entries = app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch_log_snapshot();
        assert_eq!(entries.len(), 1, "启用只跑一次 tick:{entries:?}");
        assert_eq!(entries[0].plugin_id, "com.example.ticker");
        assert_eq!(entries[0].event_type, "tick");
        assert_eq!(entries[0].result, "success", "{entries:?}");
        assert_eq!(app.db.get("plugin.com.example.ticker:called").as_deref(), Some("1"));
    }

    /// 启动时的预加载**不**触发 tick:那条路径恢复的是「已启用」这件事本身,
    /// 不是一次启用动作——否则每重启一次,每个 tick 插件都白跑一轮。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn preloading_enabled_plugins_does_not_tick_them() {
        let app = plugin_app();
        assert_eq!(upload(&app, tick_archive("com.example.ticker")).await.status(), StatusCode::OK);
        let id = app.db.list_plugins().unwrap()[0].id;
        app.db.set_plugin_enabled(id, true).unwrap();

        app.plugins.write().unwrap_or_else(|e| e.into_inner()).init(&app);
        assert!(app.plugins.read().unwrap_or_else(|e| e.into_inner()).is_loaded(id), "预加载要把它装进来");
        assert_eq!(app.db.get("plugin.com.example.ticker:called"), None, "预加载不该触发 tick");
        assert!(
            app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch_log_snapshot().is_empty(),
            "预加载不该留 tick 记录"
        );
    }

    /// manifest 声明了必填配置时,「测试」前宿主先预检:缺项直接 400 点名,而不是
    /// 让插件回来一个 `other:2` 让操作者猜。空值算没填——必须与 `host_kv_get` 对
    /// 「无值或空」都返回 0 的语义一致,否则会出现「预检说填好了、插件说没有」。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_plugin_asks_for_required_config_before_dispatching() {
        let app = plugin_app();
        let manifest = format!(
            "{}[[kv]]\nkey = \"bot_token\"\nlabel = \"Bot Token\"\nrequired = true\n\
             [[kv]]\nkey = \"note\"\n",
            plugin_manifest("com.example.needs-config", 2)
        );
        assert_eq!(upload(&app, plugin_archive(&manifest)).await.status(), StatusCode::OK);
        let id = app.db.list_plugins().unwrap()[0].id;
        assert_eq!(enable_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);

        let set = |key: &str, value: &str| {
            set_plugin_kv(
                Admin,
                State(app.clone()),
                Path((id, key.to_owned())),
                Json(json!({ "value": value })),
            )
        };
        let test = || test_plugin(Admin, State(app.clone()), Path(id));

        // 必填的没填:400 点名「标签(key)」;非必填的不点名。
        let refused = test().await;
        assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
        let text =
            String::from_utf8_lossy(&axum::body::to_bytes(refused.into_body(), usize::MAX).await.unwrap())
                .into_owned();
        assert!(text.contains("Bot Token(bot_token)"), "要点名标签与 key,实际:{text}");
        assert!(!text.contains("note"), "非必填项不点名,实际:{text}");

        // 空串等于没填:预检必须与插件运行时看到的一致。
        assert_eq!(set("bot_token", "").await.status(), StatusCode::OK);
        assert_eq!(test().await.status(), StatusCode::BAD_REQUEST, "空值不算填过");
        // 拦下的是整批测试,不是「第一条」:一条都不该派发出去。
        assert!(
            app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch_log_snapshot().is_empty(),
            "缺必填配置时不该派发任何一条"
        );

        // 填上真值:放行,回到真实的派发路径(MINIMAL_WAT 返回 0)。
        assert_eq!(set("bot_token", "123:abc").await.status(), StatusCode::OK);
        let ok = test().await;
        assert_eq!(ok.status(), StatusCode::OK);
        assert_eq!(body_of(ok).await["results"][0]["result"], "success");
    }

    /// 「测试」的合成事件:宿主自身事件用真实结构(字段齐全),插件事件回放
    /// manifest 里声明的样例。node_id 一律 0——真实节点 id 为正,订阅节点事件的
    /// 插件据此忽略它,一次测试才不会在别人的数据里留下残留。
    #[test]
    fn synthetic_events_carry_real_host_payloads_and_replay_samples() {
        let text = "plugin_id = \"com.example.synth\"\nname = \"t\"\nversion = \"1.0.0\"\n\
                    abi_version = 2\nsubscribes = [\"agent_offline\", \"agent_online\", \"node_added\", \
                    \"node_deleted\", \"plugin_expiry_soon\", \"plugin_ghost\"]\n\
                    [[sample]]\nname = \"plugin_expiry_soon\"\npayload = '{\"node_id\":7,\"name\":\"edge-1\"}'\n";
        let events = synthetic_events(&Manifest::parse(text).unwrap(), 1_000);
        let names: Vec<&str> = events.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(
            names,
            [
                "agent_offline",
                "agent_online",
                "node_added",
                "node_deleted",
                "plugin_expiry_soon",
                "plugin_ghost"
            ],
            "按 subscribes 的顺序逐条来,面板才好逐条展示"
        );
        match &events[0].1 {
            Some(Event::AgentOffline { node_id, name, observed_at, last_seen_at }) => {
                assert_eq!(
                    (*node_id, observed_at, last_seen_at),
                    (0, &1_000, &700),
                    "留一段静默时长,离线文案才有东西可渲染"
                );
                assert_eq!(name, "test");
            }
            other => panic!("应当是真实结构的 AgentOffline,实际 {other:?}"),
        }
        match &events[1].1 {
            Some(Event::AgentOnline { node_id, observed_at, .. }) => {
                assert_eq!((*node_id, *observed_at), (0, 1_000))
            }
            other => panic!("应当是 AgentOnline,实际 {other:?}"),
        }
        // 新增与删除共用一个 created_at:财务插件按「身份相符」判要不要真删,
        // 一配对净效果为零,不会留下一条名为 test 的假节点。
        match (&events[2].1, &events[3].1) {
            (
                Some(Event::NodeAdded { created_at: added, .. }),
                Some(Event::NodeDeleted { created_at, .. }),
            ) => {
                assert_eq!((added, created_at), (&1_000, &1_000));
            }
            other => panic!("应当是 NodeAdded + NodeDeleted,实际 {other:?}"),
        }
        match &events[4].1 {
            Some(Event::Plugin { name, payload }) => {
                assert_eq!(name, "plugin_expiry_soon");
                assert_eq!(payload["name"], "edge-1", "回放的是插件在 manifest 里声明的样例");
            }
            other => panic!("应当回放声明的样例,实际 {other:?}"),
        }
        assert!(events[5].1.is_none(), "没声明样例的插件事件记成「测不了」,而不是编一个空载荷去派发");
    }

    /// 「测试」按订阅逐条真派发:一次点击把每条订阅都过一遍真实路径,声明的样例
    /// 回放给插件。订阅顺序即结果顺序。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_plugin_walks_every_subscribed_event() {
        let app = plugin_app();
        let manifest = format!(
            "{}[[sample]]\nname = \"plugin_expiry_soon\"\npayload = '{{\"node_id\":0,\"name\":\"test\"}}'\n",
            plugin_manifest("com.example.walks", 2)
        );
        assert_eq!(upload(&app, plugin_archive(&manifest)).await.status(), StatusCode::OK);
        let id = app.db.list_plugins().unwrap()[0].id;
        assert_eq!(enable_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);

        let body = body_of(test_plugin(Admin, State(app.clone()), Path(id)).await).await;
        assert_eq!(body["plugin_id"], "com.example.walks");
        let results = body["results"].as_array().expect("逐条结果").clone();
        let names: Vec<&str> = results.iter().map(|r| r["event"].as_str().unwrap()).collect();
        assert_eq!(names, ["agent_offline", "plugin_expiry_soon"], "plugin_manifest 订阅的这两条");
        for entry in &results {
            assert_eq!(entry["result"], "success", "MINIMAL_WAT 对什么都返回 0:{entry}");
            assert!(entry["elapsed_ms"].as_u64().is_some());
        }
    }

    /// 没声明 `[[sample]]` 的插件事件:明说「测不了」,而不是编一个空载荷派发——
    /// 空载荷到插件那边是解析失败(错误码 1),操作员会以为是自己的插件坏了。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unsampled_plugin_event_is_reported_not_dispatched() {
        let app = plugin_app();
        assert_eq!(
            upload(&app, plugin_archive(&plugin_manifest("com.example.nosample", 2))).await.status(),
            StatusCode::OK
        );
        let id = app.db.list_plugins().unwrap()[0].id;
        assert_eq!(enable_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);

        let results = body_of(test_plugin(Admin, State(app.clone()), Path(id)).await).await["results"]
            .as_array()
            .expect("逐条结果")
            .clone();
        assert_eq!(results[0]["result"], "success");
        assert_eq!(results[1]["result"], "no_sample");
        assert!(
            results[1]["detail"].as_str().unwrap_or_default().contains("样例"),
            "要说清缺什么:{:?}",
            results[1]["detail"]
        );
        // 关键:那一条没有被派发出去(派发日志里只该有真正跑过的那条)。
        let logged: Vec<String> = app
            .plugins
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .dispatch_log_snapshot()
            .into_iter()
            .map(|entry| entry.event_type)
            .collect();
        assert_eq!(logged, ["agent_offline"], "没样例的那条不该进派发日志");
    }

    /// 靠 tick/page 工作、没订阅任何事件的插件:「测试」返回空结果而不是报错
    /// ——它没什么可测的,但按钮不该因此变红。(tick_archive 的 manifest 就是
    /// subscribes = [] + tick,且模块真的导出了 on_tick。)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_plugin_without_subscriptions_tests_nothing() {
        let app = plugin_app();
        assert_eq!(upload(&app, tick_archive("com.example.quiet")).await.status(), StatusCode::OK);
        let id = app.db.list_plugins().unwrap()[0].id;
        assert_eq!(enable_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);

        let body = body_of(test_plugin(Admin, State(app.clone()), Path(id)).await).await;
        assert_eq!(body["results"], json!([]), "没有订阅就没有可测的:空结果,不是错误");
    }

    /// 预检遇到读库失败要报 500,不能把库故障说成「你还没配」——那是一句自信而错误
    /// 的指引,会把操作员支去重填一个本来就在的值。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_broken_read_is_a_500_not_a_missing_config() {
        let app = plugin_app();
        let manifest = format!(
            "{}[[kv]]\nkey = \"bot_token\"\nrequired = true\n",
            plugin_manifest("com.example.broken-db", 2)
        );
        assert_eq!(upload(&app, plugin_archive(&manifest)).await.status(), StatusCode::OK);
        let id = app.db.list_plugins().unwrap()[0].id;
        assert_eq!(enable_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);
        // 表没了 —— 一次真实的读库失败,必须与「这一项没填」区分开。
        app.db.conn().execute("DROP TABLE setting", []).unwrap();
        assert_eq!(
            test_plugin(Admin, State(app.clone()), Path(id)).await.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "库故障不该被报成「缺少必填配置」"
        );
    }

    /// 列表接口把 manifest 的 `[[kv]]` 透给面板:key/label/required/hint,
    /// 缺省的 label/hint 是 null(与 `page` 的约定一致)。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_plugin_declares_its_config_for_the_panel() {
        let app = plugin_app();
        let manifest = format!(
            "{}[[kv]]\nkey = \"bot_token\"\nlabel = \"Bot Token\"\nrequired = true\n\
             hint = \"向 @BotFather 申请\"\n[[kv]]\nkey = \"tpl\"\ntype = \"textarea\"\n\
             default = \"⏰ {{name}} 到期\"\n[[kv]]\nkey = \"chat_id\"\n",
            plugin_manifest("com.example.declares", 2)
        );
        assert_eq!(upload(&app, plugin_archive(&manifest)).await.status(), StatusCode::OK);
        let body = body_of(list_plugins(Admin, State(app.clone())).await).await;
        assert_eq!(
            body[0]["config"],
            json!([
                {"key": "bot_token", "label": "Bot Token", "required": true, "hint": "向 @BotFather 申请", "type": "text", "default": null},
                {"key": "tpl", "label": null, "required": false, "hint": null, "type": "textarea", "default": "⏰ {name} 到期"},
                {"key": "chat_id", "label": null, "required": false, "hint": null, "type": "text", "default": null},
            ])
        );
        // 没声明 [[kv]] 的插件解析出空表:面板照旧,不显示配置提示。
        assert_eq!(
            upload(&app, plugin_archive(&plugin_manifest("com.example.plain", 2))).await.status(),
            StatusCode::OK
        );
        let body = body_of(list_plugins(Admin, State(app.clone())).await).await;
        // 列表按 uploaded_at DESC, id DESC:后传的在前。
        assert_eq!(body[0]["plugin_id"], "com.example.plain");
        assert_eq!(body[0]["config"], json!([]));
    }

    /// 删除把三处状态一起收:db 行、kv 行、内存里的实例;不存在的行是 404。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deleting_a_plugin_takes_its_kv_with_it() {
        let app = plugin_app();
        assert_eq!(
            delete_plugin(Admin, State(app.clone()), Path(9999)).await.status(),
            StatusCode::NOT_FOUND
        );

        assert_eq!(
            upload(&app, plugin_archive(&plugin_manifest("com.example.gone", 2))).await.status(),
            StatusCode::OK
        );
        let id = app.db.list_plugins().unwrap()[0].id;
        assert_eq!(enable_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);
        assert_eq!(
            set_plugin_kv(
                Admin,
                State(app.clone()),
                Path((id, "bot_token".to_owned())),
                Json(json!({"value": "secret"})),
            )
            .await
            .status(),
            StatusCode::OK
        );
        assert_eq!(app.db.get("plugin.com.example.gone:bot_token").as_deref(), Some("secret"));

        assert_eq!(delete_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::NO_CONTENT);
        assert!(app.db.get_plugin(id).unwrap().is_none());
        assert_eq!(app.db.get("plugin.com.example.gone:bot_token"), None, "kv 行随插件删除");
        assert!(!app.plugins.read().unwrap_or_else(|e| e.into_inner()).is_loaded(id));
        assert_eq!(list_plugin_kv(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::NOT_FOUND);
    }

    /// kv 的往返与校验:key 非空、不含 ':'、不超 128 字节;value 不超 8 KiB
    /// (与 host_kv_set 同限);body 必须是 {"value": "..."}。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plugin_kv_round_trips_and_refuses_the_same_things_the_host_does() {
        let app = plugin_app();
        assert_eq!(
            upload(&app, plugin_archive(&plugin_manifest("com.example.kv", 2))).await.status(),
            StatusCode::OK
        );
        let id = app.db.list_plugins().unwrap()[0].id;
        let put = |key: &str, value: Value| {
            set_plugin_kv(Admin, State(app.clone()), Path((id, key.to_owned())), Json(value))
        };

        assert_eq!(
            put("webhook", json!({"value": "https://example.com/hook"})).await.status(),
            StatusCode::OK
        );
        // 覆盖写同一个 key。
        assert_eq!(
            put("webhook", json!({"value": "https://example.com/other"})).await.status(),
            StatusCode::OK
        );
        let listed = body_of(list_plugin_kv(Admin, State(app.clone()), Path(id)).await).await;
        assert_eq!(listed, json!([{"key": "webhook", "value": "https://example.com/other"}]));

        // key 为空或含 ':',value 超限,body 形状不对:全部 400,且不落库。
        assert_eq!(put("   ", json!({"value": "x"})).await.status(), StatusCode::BAD_REQUEST);
        assert_eq!(put("a:b", json!({"value": "x"})).await.status(), StatusCode::BAD_REQUEST);
        let long = "k".repeat(plugin::KV_KEY_MAX + 1);
        assert_eq!(put(&long, json!({"value": "x"})).await.status(), StatusCode::BAD_REQUEST);
        let big = "v".repeat(plugin::KV_VALUE_MAX + 1);
        assert_eq!(put("k", json!({"value": big})).await.status(), StatusCode::BAD_REQUEST);
        assert_eq!(put("k", json!({"not_value": "x"})).await.status(), StatusCode::BAD_REQUEST);
        assert_eq!(put("k", json!({"value": 42})).await.status(), StatusCode::BAD_REQUEST);
        // 校验失败的写一个都没落库。
        assert_eq!(app.db.plugin_kv("com.example.kv").unwrap().len(), 1);

        // 不存在的插件行是 404。
        assert_eq!(
            set_plugin_kv(
                Admin,
                State(app.clone()),
                Path((9999, "k".to_owned())),
                Json(json!({"value": "x"})),
            )
            .await
            .status(),
            StatusCode::NOT_FOUND
        );
    }

    /// 点号碰撞:`com.example` 与 `com.example.tg-notify` 的 kv 互相看不见,
    /// 删除前者也不动后者的行——前缀匹配止于 `:`。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plugin_kv_namespaces_do_not_leak_across_dotted_prefixes() {
        let app = plugin_app();
        for plugin_id in ["com.example", "com.example.tg-notify"] {
            assert_eq!(
                upload(&app, plugin_archive(&plugin_manifest(plugin_id, 2))).await.status(),
                StatusCode::OK
            );
        }
        let rows = app.db.list_plugins().unwrap();
        // list_plugins 按 uploaded_at DESC 排,靠 plugin_id 找回行号。
        let id_of = |pid: &str| rows.iter().find(|r| r.plugin_id == pid).unwrap().id;
        let (a, b) = (id_of("com.example"), id_of("com.example.tg-notify"));

        assert_eq!(
            set_plugin_kv(
                Admin,
                State(app.clone()),
                Path((a, "bot_token".to_owned())),
                Json(json!({"value": "of-a"}))
            )
            .await
            .status(),
            StatusCode::OK
        );
        // B 看不到 A 的 key,尽管 A 的 plugin_id 是 B 的前缀。
        assert_eq!(body_of(list_plugin_kv(Admin, State(app.clone()), Path(b)).await).await, json!([]));
        // B 自己写一个同名 key,也不覆盖 A 的。
        assert_eq!(
            set_plugin_kv(
                Admin,
                State(app.clone()),
                Path((b, "bot_token".to_owned())),
                Json(json!({"value": "of-b"}))
            )
            .await
            .status(),
            StatusCode::OK
        );
        assert_eq!(app.db.get("plugin.com.example:bot_token").as_deref(), Some("of-a"));

        // 删除 A 连带清掉它的 kv,B 的原样保留。
        assert_eq!(delete_plugin(Admin, State(app.clone()), Path(a)).await.status(), StatusCode::NO_CONTENT);
        assert_eq!(app.db.get("plugin.com.example:bot_token"), None);
        let left = body_of(list_plugin_kv(Admin, State(app.clone()), Path(b)).await).await;
        assert_eq!(left, json!([{"key": "bot_token", "value": "of-b"}]));
    }

    /// kv 的单行删除(R13):删掉点名的那一行,别的行不动;key 校验与 set
    /// 同一套;插件行不存在是 404。删不存在的行同样是 204——两处面板同时
    /// 打开,后点的那个也达成了目标。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_plugin_kv_row_can_be_deleted_on_its_own() {
        let app = plugin_app();
        assert_eq!(
            upload(&app, plugin_archive(&plugin_manifest("com.example.kv-del", 2))).await.status(),
            StatusCode::OK
        );
        let id = app.db.list_plugins().unwrap()[0].id;
        let put = |key: &str| {
            set_plugin_kv(Admin, State(app.clone()), Path((id, key.to_owned())), Json(json!({"value": "v"})))
        };
        assert_eq!(put("webhook").await.status(), StatusCode::OK);
        assert_eq!(put("fallback").await.status(), StatusCode::OK);
        let del = |key: &str| delete_plugin_kv_route(Admin, State(app.clone()), Path((id, key.to_owned())));

        assert_eq!(del("webhook").await.status(), StatusCode::NO_CONTENT);
        assert_eq!(app.db.get("plugin.com.example.kv-del:webhook"), None, "点名的那一行删掉");
        let left = body_of(list_plugin_kv(Admin, State(app.clone()), Path(id)).await).await;
        assert_eq!(left, json!([{"key": "fallback", "value": "v"}]), "别的行原样保留");

        // 已经不存在的行:同样 204,重试幂等。
        assert_eq!(del("webhook").await.status(), StatusCode::NO_CONTENT);

        // key 校验与 set 同一套:空、首尾空白、含 ':'、超长。
        for bad_key in ["   ", " bot", "bot ", "a:b", &"k".repeat(plugin::KV_KEY_MAX + 1)] {
            assert_eq!(del(bad_key).await.status(), StatusCode::BAD_REQUEST, "{bad_key:?}");
        }
        // 插件行不存在是 404,不是静默成功。
        assert_eq!(
            delete_plugin_kv_route(Admin, State(app.clone()), Path((9999, "k".to_owned()))).await.status(),
            StatusCode::NOT_FOUND
        );
    }

    /// 派发日志按 plugin_id 过滤:两个插件各自测试过,每个的日志只有自己的。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_dispatch_log_is_filtered_per_plugin() {
        let app = plugin_app();
        for plugin_id in ["com.example.alpha", "com.example.beta"] {
            assert_eq!(
                upload(&app, plugin_archive(&plugin_manifest(plugin_id, 2))).await.status(),
                StatusCode::OK
            );
        }
        let rows = app.db.list_plugins().unwrap();
        let id_of = |pid: &str| rows.iter().find(|r| r.plugin_id == pid).unwrap().id;
        let (alpha, beta) = (id_of("com.example.alpha"), id_of("com.example.beta"));
        for id in [alpha, beta, alpha] {
            assert_eq!(enable_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);
            assert_eq!(test_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);
        }

        let log = body_of(
            plugin_dispatch_log(Admin, State(app.clone()), Path(alpha), Query(LogPage::default())).await,
        )
        .await;
        let entries = log["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 2, "alpha 测试了两次:{log}");
        assert_eq!(log["total"], 2, "分页总数按插件过滤后计:{log}");
        assert_eq!(log["page"], 1, "缺省落在第一页:{log}");
        assert_eq!(log["page_size"], 50, "缺省单页 50 条:{log}");
        assert!(entries.iter().all(|e| e["plugin_id"] == "com.example.alpha"), "{log}");
        // 每次「测试」派发的是它订阅的那条宿主事件。plugin_manifest 订阅两条,其中
        // plugin_expiry_soon 是插件事件而 manifest 没给它声明样例,所以不派发——
        // 真跑过的只有 agent_offline。
        assert!(
            entries.iter().all(|e| e["result"] == "success" && e["event_type"] == "agent_offline"),
            "{log}"
        );
        // 快照新 → 旧:最新一条在头部(两次测试可能落在同一秒,只比先后)。
        assert!(entries[0]["at"].as_i64() >= entries[1]["at"].as_i64(), "{log}");

        assert_eq!(
            plugin_dispatch_log(Admin, State(app.clone()), Path(9999), Query(LogPage::default()))
                .await
                .status(),
            StatusCode::NOT_FOUND
        );
    }

    /// 分页把环形缓冲里该插件的全部分派记录翻出来,不再止步旧实现的「最近
    /// 100 条」:第一页是最新的一段,往后翻按同一顺序续,翻过头给空页而
    /// total 仍然报全量。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_dispatch_log_paginates_through_the_whole_ring() {
        let app = plugin_app();
        assert_eq!(
            upload(&app, plugin_archive(&plugin_manifest("com.example.alpha", 2))).await.status(),
            StatusCode::OK
        );
        let id = app.db.list_plugins().unwrap()[0].id;
        for _ in 0..3 {
            assert_eq!(enable_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);
            assert_eq!(test_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);
        }

        let call = |page: u32, size: u32| {
            let app = app.clone();
            async move {
                body_of(
                    plugin_dispatch_log(
                        Admin,
                        State(app),
                        Path(id),
                        Query(LogPage { page: Some(page), page_size: Some(size) }),
                    )
                    .await,
                )
                .await
            }
        };

        // 三条记录、单页 2 条:第一页装最新两条,第二页是剩下的最旧一条。
        let first = call(1, 2).await;
        assert_eq!(first["total"], 3, "{first}");
        assert_eq!(first["page"], 1, "{first}");
        assert_eq!(first["page_size"], 2, "{first}");
        assert_eq!(first["entries"].as_array().unwrap().len(), 2, "{first}");
        let second = call(2, 2).await;
        assert_eq!(second["entries"].as_array().unwrap().len(), 1, "{second}");
        assert_eq!(second["total"], 3, "翻页不动 total:{second}");
        // 快照新 → 旧跨页保持:第一页末条不早于第二页首条(可能同秒)。
        let first_ats = first["entries"].as_array().unwrap();
        let second_ats = second["entries"].as_array().unwrap();
        assert!(
            first_ats.last().unwrap()["at"].as_i64() >= second_ats[0]["at"].as_i64(),
            "跨页顺序应仍是新 → 旧:{first} {second}"
        );

        // 翻过头的页是空的,total 不变——前端据此知道没有更多页。
        let past = call(9, 2).await;
        assert_eq!(past["entries"].as_array().unwrap().len(), 0, "{past}");
        assert_eq!(past["total"], 3, "{past}");

        // 单页 0 条与超过上限的 page_size 都就近夹取,不报错。
        let clamped = call(1, 0).await;
        assert_eq!(clamped["page_size"], 1, "{clamped}");
        assert_eq!(clamped["entries"].as_array().unwrap().len(), 1, "{clamped}");
        let clamped = call(1, 9999).await;
        assert_eq!(clamped["page_size"], 500, "{clamped}");
        assert_eq!(clamped["entries"].as_array().unwrap().len(), 3, "{clamped}");
    }

    /// restore 整库替换 plugin 表,内存里的 Registry 也必须跟着按还原后的
    /// 行重建:置之不理的话,备份里没有的插件会继续收事件,备份里 enabled
    /// 的插件永远装不进内存(Registry 只在启动 init 一次)。文件库而不是
    /// :memory:,因为还原路径本身就是文件操作。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_restore_rebuilds_the_plugin_registry_from_the_restored_table() {
        let dir = std::env::temp_dir().join(format!("monitor-restore-plugin-{}", &random_token()[..16]));
        std::fs::create_dir_all(&dir).unwrap();
        let live = dir.join("live.db").to_string_lossy().into_owned();
        let app = std::sync::Arc::new(App::for_test(Db::open(&live).unwrap()));
        fn registry(app: &App) -> std::sync::RwLockReadGuard<'_, plugin::Registry> {
            app.plugins.read().unwrap_or_else(|e| e.into_inner())
        }

        // 备份里有一个 enabled 的插件。
        let wasm = wat::parse_str(MINIMAL_WAT).unwrap();
        let backed_up = app
            .db
            .create_plugin(
                "com.example.backup",
                "Test Plugin",
                "1.0.0",
                &plugin_manifest("com.example.backup", 2),
                &wasm,
                "sha",
            )
            .unwrap();
        app.db.set_plugin_enabled(backed_up.id, true).unwrap();
        // 启动等价物:main 在 Arc::new 之后预加载一次,enabled 的插件装进内存。
        app.plugins.write().unwrap_or_else(|e| e.into_inner()).init(&app);
        assert!(registry(&app).is_loaded(backed_up.id), "预加载装上了备份里的插件");

        let copy = format!("{live}.copy");
        app.db.backup_into(&copy).unwrap();
        let bytes = std::fs::read(&copy).unwrap();
        std::fs::remove_file(&copy).unwrap();
        assert!(registry(&app).is_loaded(backed_up.id), "预加载装上了备份里的插件");

        // 备份之后:面板停用了备份里的插件,又传了另一个并启用,db 与内存
        // 各自一致。备份里的行留着不删——SQLite 会把删掉的最高行号让给
        // 下一个插入,两个插件就会共用同一个 id,断言分不清谁是谁。
        app.db.set_plugin_enabled(backed_up.id, false).unwrap();
        app.plugins.write().unwrap_or_else(|e| e.into_inner()).disable_plugin(backed_up.id);
        let after = app
            .db
            .create_plugin(
                "com.example.after",
                "Test Plugin",
                "1.0.0",
                &plugin_manifest("com.example.after", 2),
                &wasm,
                "sha",
            )
            .unwrap();
        assert_eq!(after.id, backed_up.id + 1, "fixture 只在两个 id 不同时才有意义");
        app.db.set_plugin_enabled(after.id, true).unwrap();
        app.plugins.write().unwrap_or_else(|e| e.into_inner()).enable_plugin(&app, after.id).unwrap();
        assert!(!registry(&app).is_loaded(backed_up.id) && registry(&app).is_loaded(after.id));

        let done = db_restore(
            Admin,
            State(app.clone()),
            Query(Chunk { offset: 0, total: bytes.len() as u64 }),
            HeaderMap::new(),
            axum::body::Body::from(bytes),
        )
        .await;
        assert_eq!(done.status(), StatusCode::OK);

        // 还原后的注册表以还原库为准:备份里的插件重新装进内存,备份之后
        // 传的不再在。
        assert!(registry(&app).is_loaded(backed_up.id), "备份里 enabled 的插件要重新加载");
        assert!(!registry(&app).is_loaded(after.id), "备份里没有的插件不能留在内存里");
        let enabled: Vec<String> =
            app.db.enabled_plugins().unwrap().into_iter().map(|r| r.plugin_id).collect();
        assert_eq!(enabled, vec!["com.example.backup".to_owned()]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    // ---- 页面协议(U5) ----

    /// 声明 page 的插件:render_page 与 on_action 各自经 host_resp_alloc
    /// 拿缓冲、写入一段 JSON、返回长度。
    const PAGE_WAT: &str = r#"
(module
  (import "host" "resp_alloc" (func $alloc (param i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "{\"title\":\"Finance\"}")
  (func (export "__alloc") (param i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32) (i32.const 0))
  (func (export "on_action") (param i32 i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (call $alloc (i32.const 64)))
    (memory.copy (local.get $ptr) (i32.const 1024) (i32.const 19))
    (i32.const 19))
  (func (export "render_page") (param i32 i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (call $alloc (i32.const 64)))
    (memory.copy (local.get $ptr) (i32.const 1024) (i32.const 19))
    (i32.const 19)))"#;

    const PAGE_MANIFEST: &str = r#"
plugin_id = "com.example.paged"
name = "Paged"
version = "1.0.0"
abi_version = 2
subscribes = []

[page]
title = "Finance"
"#;

    fn page_archive() -> Vec<u8> {
        tarball(&[
            ("plugin.toml", PAGE_MANIFEST.as_bytes().to_vec()),
            ("plugin.wasm", wat::parse_str(PAGE_WAT).unwrap()),
        ])
    }

    /// 声明 page 的插件:上传、启用后 GET page 返回插件渲染的 JSON 描述;
    /// 未声明 page 的插件 404。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_page_plugin_renders_and_others_404() {
        let app = plugin_app();
        assert_eq!(upload(&app, page_archive()).await.status(), StatusCode::OK);
        let id = app.db.list_plugins().unwrap()[0].id;
        // 未启用:报 400「插件未启用」而不是 404「没声明页面」——面板按存储的
        // manifest 显示「页面」按钮,运维该去启用插件,而不是去改 manifest。
        assert_eq!(
            render_plugin_page(Admin, State(app.clone()), Path(id)).await.status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(enable_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);
        let resp = render_plugin_page(Admin, State(app.clone()), Path(id)).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_of(resp).await;
        assert_eq!(body["title"], "Finance", "插件渲染的页面描述透传给前端");

        // action 回环:把 body 交给 on_action,返回它写的 JSON。
        let resp = plugin_page_action(
            Admin,
            State(app.clone()),
            Path(id),
            axum::body::Bytes::from_static(b"{\"action\":\"x\"}"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_of(resp).await["title"], "Finance");

        // 未声明 page 的插件:启用后仍是 404(声明缺失),与「未启用」的 400 区分。
        assert_eq!(
            upload(&app, plugin_archive(&plugin_manifest("com.example.nopage", 2))).await.status(),
            StatusCode::OK
        );
        let plain =
            app.db.list_plugins().unwrap().iter().find(|p| p.plugin_id == "com.example.nopage").unwrap().id;
        assert_eq!(enable_plugin(Admin, State(app.clone()), Path(plain)).await.status(), StatusCode::OK);
        assert_eq!(
            render_plugin_page(Admin, State(app.clone()), Path(plain)).await.status(),
            StatusCode::NOT_FOUND
        );
    }

    /// 页面钩子的 fuel 预算与事件派发分开(README「资源限制」)。一页列出几百台
    /// 机器的财务记录是几百万 fuel 的活,套用按有界事件载荷定的 1,000,000 会
    /// trap,页面端点回 502——部署里点「财务统计」报 502 就是这条路径。
    /// 这里用一个空转约 270 万 fuel 的页面钩子证明它走的是宽预算那一档。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_page_hook_that_burns_millions_of_fuel_still_renders() {
        // 30 万次循环 × 每轮约 9 条指令 ≈ 270 万 fuel:远超派发那档 1,000,000,
        // 仍远低于钩子那档 20,000,000。换成派发那档会 trap 成 502。
        const HEAVY_PAGE_WAT: &str = r#"
(module
  (import "host" "resp_alloc" (func $alloc (param i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "{\"title\":\"Finance\"}")
  (func (export "__alloc") (param i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32) (i32.const 0))
  (func $spin (local $i i32)
    (block $done
      (loop $again
        (br_if $done (i32.ge_u (local.get $i) (i32.const 300000)))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $again))))
  (func (export "render_page") (param i32 i32) (result i32)
    (local $ptr i32)
    (call $spin)
    (local.set $ptr (call $alloc (i32.const 64)))
    (memory.copy (local.get $ptr) (i32.const 1024) (i32.const 19))
    (i32.const 19))
  (func (export "on_action") (param i32 i32) (result i32) (i32.const 0)))"#;

        let app = plugin_app();
        let archive = tarball(&[
            ("plugin.toml", PAGE_MANIFEST.as_bytes().to_vec()),
            ("plugin.wasm", wat::parse_str(HEAVY_PAGE_WAT).unwrap()),
        ]);
        assert_eq!(upload(&app, archive).await.status(), StatusCode::OK);
        let id = app.db.list_plugins().unwrap()[0].id;
        assert_eq!(enable_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);

        let resp = render_plugin_page(Admin, State(app.clone()), Path(id)).await;
        assert_eq!(resp.status(), StatusCode::OK, "重活的页面钩子不该被派发那档预算截断");
        assert_eq!(body_of(resp).await["title"], "Finance");
    }

    /// 未声明 cleanup 的插件,清理端点 404(KTD11)。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cleanup_404s_without_the_declaration() {
        let app = plugin_app();
        assert_eq!(
            upload(&app, plugin_archive(&plugin_manifest("com.example.noclean", 2))).await.status(),
            StatusCode::OK
        );
        let id = app.db.list_plugins().unwrap()[0].id;
        // 未启用 → 400「未启用」;启用后没声明 cleanup → 404「没声明清理能力」。
        // 两者是不同的恢复动作,不能混成一个码。
        assert_eq!(
            plugin_cleanup(Admin, State(app.clone()), Path(id)).await.status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(enable_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);
        assert_eq!(plugin_cleanup(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::NOT_FOUND);
    }
}
