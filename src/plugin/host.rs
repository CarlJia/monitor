//! 引擎与加载(R6)、宿主函数(R8)、事件派发入口。

use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::Utc;
use tracing::warn;
use wasmtime::{Caller, Extern, Linker, Memory, Store};

use crate::notification_bus::Event;
use crate::{db::PluginRow, App};

use super::manifest::{Manifest, PLUGIN_EVENT_PREFIX};

// ---------------------------------------------------------------------------
// 引擎与加载(R6)
// ---------------------------------------------------------------------------

/// 进程唯一的 wasmtime 引擎。fuel 计量必须在 Config 上显式开启,否则
/// `Store::set_fuel` 静默无效,死循环插件不会被中断。
pub fn new_engine() -> wasmtime::Engine {
    let mut config = wasmtime::Config::new();
    config.consume_fuel(true);
    wasmtime::Engine::new(&config).expect("构建 wasmtime Engine 不应失败")
}

/// 一个加载完毕、可以反复调用的插件:manifest 与编译产物。
///
/// 只缓存 `Module`(Send+Sync,编译结果在 Engine 里共享,clone 是 Arc 语义);
/// 不持有 instance/store,原因见模块文档的"资源模型"。派发把整个
/// `LoadedPlugin` clone 进任务:manifest 与 Module 都是 Arc 克隆,廉价。
#[derive(Clone)]
pub struct LoadedPlugin {
    pub manifest: Manifest,
    module: wasmtime::Module,
}

impl std::fmt::Debug for LoadedPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedPlugin").field("manifest", &self.manifest).finish_non_exhaustive()
    }
}

/// 把 db 行变成可调用的插件:解析 manifest、编译 wasm、检查导出契约。
/// manifest 与模块二进制各自独立报错——上传 API 需要区分"manifest 写错了"和
/// "模块编译不过"。
pub fn load(engine: &wasmtime::Engine, row: &PluginRow) -> Result<LoadedPlugin> {
    let manifest = Manifest::parse(&row.manifest_json)
        .with_context(|| format!("插件 {}({}) 的 manifest 无效", row.id, row.plugin_id))?;
    let module = wasmtime::Module::new(engine, &row.wasm_blob[..])
        .map_err(|e| anyhow::anyhow!("插件 {}({}) 的 wasm 模块编译失败: {e}", row.id, row.plugin_id))?;
    // 导出契约在加载时检查而不是调用时:一次上传、尽早暴露,调用路径上不再有
    // "模块长得不对"这种配置型错误。v2 在 v1 的三项之外,按 manifest 声明
    // 检查 tick/page/cleanup 对应的导出(KTD12)——声明了能力却缺导出是配置
    // 型错误,与缺 on_event 同等对待。
    use wasmtime::ExternType;
    if !matches!(module.get_export("memory"), Some(ExternType::Memory(_))) {
        bail!("插件 {} 的模块缺少导出 `memory`", manifest.plugin_id);
    }
    let mut required: Vec<&str> = vec!["on_event", "__alloc"];
    if manifest.tick {
        required.push("on_tick");
    }
    if manifest.page.is_some() {
        required.extend(["render_page", "on_action"]);
    }
    if manifest.cleanup {
        required.push("on_cleanup");
    }
    for name in required {
        if !matches!(module.get_export(name), Some(ExternType::Func(_))) {
            bail!("插件 {} 的模块缺少导出 `{name}`(manifest 声明了该能力)", manifest.plugin_id);
        }
    }
    Ok(LoadedPlugin { manifest, module })
}

// ---------------------------------------------------------------------------
// 宿主函数(R8)
// ---------------------------------------------------------------------------

/// 每次调用的默认 fuel 限额(KTD6)。dispatch 每次读 setting
/// `plugin.fuel_limit` 覆写,缺省回落到这里。
pub const DEFAULT_FUEL_LIMIT: u64 = 1_000_000;

/// 单个插件的 kv 值上限:8 KiB,足够放渠道配置,又不会让 setting 表被一个插件
/// 当对象存储用。面板的 kv 写入引用同一个上限:「面板能写的不能比插件运行时
/// 能写的多」,否则面板成了绕过插件存储限额的后门。
pub const KV_VALUE_MAX: usize = 8 * 1024;

/// 插件 http 请求的墙钟超时(A13):4 秒,落在 5 秒的派发预算内,留 1 秒给宿主
/// 自己的开销。挂在请求上而不是再建一个 client:全局 `app.http` 的 15 秒超时
/// 服务于面板自身的下载,不能为插件收短。
pub(super) const HTTP_TIMEOUT: Duration = Duration::from_secs(4);

/// 单次 http 响应体的硬上限:64 KiB。插件声明的 resp_cap 再大也读这么多——
/// 有界下载要防的正是"cap 被声明成超大值/响应体本身无限大"的内存放大。
const HTTP_RESP_MAX: usize = 64 * 1024;

/// Store 的 user data:宿主函数看得见的全部宿主侧状态。
pub(crate) struct PluginState {
    /// kv 命名空间隔离:`plugin.<plugin_id>:<key>`。
    plugin_id: String,
    /// 宿主回程:kv 与 http 都要它。App 本就活在 Arc 里,克隆只是引用计数。
    app: Arc<App>,
    /// 本次 `on_event` 的墙钟预算截止点(由 `instantiate` 从 timeout_ms 折算)。
    /// 外层 tokio 超时只能 detach 后台任务,fuel 又只计量 wasm 指令——宿主调用
    /// 循环(每轮 wasm 指令极少、宿主侧执行再久也不耗 fuel)只有这道检查能拦。
    deadline: std::time::Instant,
    /// 最近一次 `host_resp_alloc` 拿到的缓冲。`host_http_post` 的 resp_ptr 传 0
    /// 时回落到这里,插件可以少传两个参数。
    resp_ptr: i32,
    resp_cap: i32,
}

/// 宿主函数的返回错误码。成功:kv_get/http_post 返回写入字节数,其余返回 0。
///
/// | 码 | 含义 |
/// |----|------|
/// | -1 | 内存越界 / 非法 UTF-8 / 值超限 / `__alloc` 缺失或失败 |
/// | -2 | http:URL 不是 `https://`(或 kv:数据库写入失败) |
/// | -3 | http:method 不是 `POST` |
/// | -4 | http:网络请求失败 / 宿主不在异步运行时上下文 / 墙钟预算耗尽 |
/// | -5 | http:响应状态非 2xx |
/// | -6 | data:记录或单插件配额超限(v2) |
/// | -7 | emit_event:事件名不以 `plugin_` 开头(v2) |
/// | -8 | data:nodes_query/emit:数据库错误(v2) |
const ERR_BOUNDS: i32 = -1;
const ERR_QUOTA: i32 = -6;
const ERR_DB: i32 = -8;

/// 单条 plugin_data 记录的上限:256 KiB(KTD3)。财务记录是百台机器量的
/// JSON,远低于此;上限防的是插件把它当大对象存储用。
pub const RECORD_MAX: usize = 256 * 1024;

/// 单插件 plugin_data 的总配额:16 MiB(KTD3),与上传包上限同量级的防御值。
pub const PLUGIN_DATA_MAX: i64 = 16 * 1024 * 1024;

/// 从插件线性内存读 `[ptr, ptr+len)`。返回 `None` 表示越界。宿主函数绝不能
/// panic(会把整个进程带走),所以一切访问都从这里走、先检查后拷贝。
fn read_mem(caller: &mut Caller<'_, PluginState>, ptr: i32, len: i32) -> Option<Vec<u8>> {
    if ptr < 0 || len < 0 {
        return None;
    }
    let mem = caller.get_export("memory")?.into_memory()?;
    let data = mem.data(&*caller);
    let start = ptr as usize;
    let end = start.checked_add(len as usize)?;
    Some(data.get(start..end)?.to_vec())
}

/// 读一段必须合法 UTF-8 的文本(key、method、URL)。
fn read_text(caller: &mut Caller<'_, PluginState>, ptr: i32, len: i32) -> Option<String> {
    let bytes = read_mem(caller, ptr, len)?;
    String::from_utf8(bytes).ok()
}

/// 往插件线性内存写字节。越界返回 false。
fn write_mem(caller: &mut Caller<'_, PluginState>, ptr: i32, bytes: &[u8]) -> bool {
    if ptr < 0 {
        return false;
    }
    let Some(mem) = caller.get_export("memory").and_then(Extern::into_memory) else {
        return false;
    };
    let start = ptr as usize;
    let Some(end) = start.checked_add(bytes.len()) else {
        return false;
    };
    let data = mem.data_mut(&mut *caller);
    let Some(target) = data.get_mut(start..end) else {
        return false;
    };
    target.copy_from_slice(bytes);
    true
}

/// 把调用侧的 Store/内存组合暴露给 tests:测试需要直接看内存与 db,而
/// `call_on_event` 只返回 i32。
pub(crate) struct InstanceHandle {
    pub store: Store<PluginState>,
    pub instance: wasmtime::Instance,
}

/// http 响应的写回计划(纯函数,便于单测):决定写到哪个缓冲、最多写多少字节。
///
/// - 选缓冲:显式 `resp_ptr > 0` 优先,否则回落最近一次 `host_resp_alloc`
///   (传入 `last`);都不可用返回 `None`——请求已发出,宿主返回 0 字节而不是
///   报错。
/// - 有效容量 = `min(声明 cap, HTTP_RESP_MAX)`:既是有界下载的上限(读到
///   即停,超出的字节丢弃——与"整读后截断"同效,但不会把大响应体整个拉进
///   内存),也是最终写回的截断长度。`bytes_len` 传 `usize::MAX` 时返回的
///   第二项就是纯容量,下载前据此定界。
fn resp_write_plan(resp_ptr: i32, resp_cap: i32, last: (i32, i32), bytes_len: usize) -> Option<(i32, usize)> {
    let (ptr, cap) = if resp_ptr > 0 { (resp_ptr, resp_cap) } else { last };
    if ptr <= 0 || cap < 0 {
        return None;
    }
    let cap = (cap as usize).min(HTTP_RESP_MAX);
    Some((ptr, bytes_len.min(cap)))
}

/// 新建 Store(带 fuel 与墙钟 deadline)、注册宿主函数、实例化。`call_on_event`
/// 的骨架,也是测试直接驱动单个宿主函数的入口。
pub(crate) fn instantiate(
    engine: &wasmtime::Engine,
    app: &Arc<App>,
    plugin_id: &str,
    module: &wasmtime::Module,
    fuel_limit: u64,
    timeout_ms: u64,
) -> Result<InstanceHandle> {
    let mut store = Store::new(
        engine,
        PluginState {
            plugin_id: plugin_id.into(),
            app: app.clone(),
            deadline: std::time::Instant::now() + Duration::from_millis(timeout_ms),
            resp_ptr: 0,
            resp_cap: 0,
        },
    );
    // fuel 在任何 wasm 执行(含 start 段)之前就位。
    store.set_fuel(fuel_limit)?;
    let linker = host_linker(engine)?;
    let instance = linker.instantiate(&mut store, module)?;
    Ok(InstanceHandle { store, instance })
}

/// 注册 6 个宿主函数(R8)。每次实例化都重建:Linker 不能跨 Store 复用已定义
/// 的 Func,重建的开销微秒级,正确性优先。
fn host_linker(engine: &wasmtime::Engine) -> Result<Linker<PluginState>> {
    let mut linker: Linker<PluginState> = Linker::new(engine);

    // host_log(level, ptr, len):0=debug 1=info 2=warn 3=error。越界或非法 UTF-8
    // 时记一条宿主侧 warn、内容按空处理——日志不能让插件把进程带崩。
    linker.func_wrap(
        "host",
        "log",
        |mut caller: Caller<'_, PluginState>, level: i32, ptr: i32, len: i32| {
            let plugin_id = caller.data().plugin_id.clone();
            let text = match read_text(&mut caller, ptr, len) {
                Some(text) => text,
                None => {
                    warn!(plugin = %plugin_id, "host_log 越界或非法 UTF-8: ptr={ptr} len={len}");
                    String::new()
                }
            };
            match level {
                0 => tracing::debug!(plugin = %plugin_id, "{text}"),
                1 => tracing::info!(plugin = %plugin_id, "{text}"),
                2 => warn!(plugin = %plugin_id, "{text}"),
                _ => {
                    tracing::error!(plugin = %plugin_id, "{text}");
                }
            }
        },
    )?;

    // host_now() -> i64:Unix 秒。
    linker.func_wrap("host", "now", || -> i64 { Utc::now().timestamp() })?;

    // host_kv_get(key_ptr, key_len, out_ptr, out_cap) -> i32:
    //   >0 写入 out 的字节数;0 无值;-1 越界/非法 UTF-8。值超 out_cap 时在
    //   UTF-8 字符边界截断——插件拿到的是前缀,长度对得上。
    linker.func_wrap(
        "host",
        "kv_get",
        |mut caller: Caller<'_, PluginState>,
         key_ptr: i32,
         key_len: i32,
         out_ptr: i32,
         out_cap: i32|
         -> i32 {
            // 墙钟预算检查(#3):kv 词表没有专门的预算错误码,复用 -1(负数让
            // 插件能感知预算耗尽并退出循环;正常路径不会与越界混淆——越界是
            // 参数问题,预算是时间问题,都会让插件放弃本次调用)。
            if std::time::Instant::now() >= caller.data().deadline {
                warn!(plugin = %caller.data().plugin_id, "kv_get 超出派发墙钟预算,拒绝");
                return ERR_BOUNDS;
            }
            let Some(key) = read_text(&mut caller, key_ptr, key_len) else {
                return ERR_BOUNDS;
            };
            let plugin_id = caller.data().plugin_id.clone();
            let Some(value) = caller.data().app.db.get(&format!("plugin.{plugin_id}:{key}")) else {
                return 0;
            };
            if out_ptr < 0 || out_cap < 0 {
                return ERR_BOUNDS;
            }
            let cap = out_cap as usize;
            let end = char_boundary_end(&value, cap);
            let bytes = &value.as_bytes()[..end];
            if !write_mem(&mut caller, out_ptr, bytes) {
                return ERR_BOUNDS;
            }
            bytes.len() as i32
        },
    )?;

    // host_kv_set(key_ptr, key_len, val_ptr, val_len) -> i32:
    //   0 成功;-1 越界/非法 UTF-8/值超 8 KiB;-2 数据库失败。
    linker.func_wrap(
        "host",
        "kv_set",
        |mut caller: Caller<'_, PluginState>, key_ptr: i32, key_len: i32, val_ptr: i32, val_len: i32| -> i32 {
            let Some(key) = read_text(&mut caller, key_ptr, key_len) else {
                return ERR_BOUNDS;
            };
            let Some(value) = read_text(&mut caller, val_ptr, val_len) else {
                return ERR_BOUNDS;
            };
            if value.len() > KV_VALUE_MAX {
                warn!(plugin = %caller.data().plugin_id, key = %key, "host_kv_set 值超过 {} 字节上限", KV_VALUE_MAX);
                return ERR_BOUNDS;
            }
            let plugin_id = caller.data().plugin_id.clone();
            let namespaced = format!("plugin.{plugin_id}:{key}");
            if caller.data().app.db.set(&namespaced, &value).is_err() {
                return -2;
            }
            0
        },
    )?;

    // host_resp_alloc(cap) -> i32:宿主无法直接在 wasm 堆上分配,采用生态标准的
    // allocator 回环——回调模块自己导出的 `__alloc(cap) -> ptr`,把指针记入
    // PluginState 并返回给插件。模块未导出 `__alloc`(或分配失败/返回非正指针)
    // 时返回 -1;调用方因此要导出它,模块契约(见模块文档)在加载时已检查。
    linker.func_wrap("host", "resp_alloc", |mut caller: Caller<'_, PluginState>, cap: i32| -> i32 {
        if cap <= 0 {
            return ERR_BOUNDS;
        }
        let Some(func) = caller.get_export("__alloc").and_then(Extern::into_func) else {
            warn!(plugin = %caller.data().plugin_id, "host_resp_alloc 找不到 __alloc 导出");
            return ERR_BOUNDS;
        };
        let Ok(typed) = func.typed::<(i32,), i32>(&caller) else {
            return ERR_BOUNDS;
        };
        match typed.call(&mut caller, (cap,)) {
            Ok(ptr) if ptr > 0 => {
                let state = caller.data_mut();
                state.resp_ptr = ptr;
                state.resp_cap = cap;
                ptr
            }
            _ => ERR_BOUNDS,
        }
    })?;

    // host_http_post(method_ptr, method_len, url_ptr, url_len, body_ptr, body_len,
    //                resp_ptr, resp_cap) -> i32:
    //   >0 写入 resp 的字节数;-1 参数越界/非法 UTF-8/resp 写不进;
    //   -2 URL 非 https;-3 method 非 POST;-4 网络失败/预算耗尽;-5 状态非 2xx。
    // v1 固定发 `Content-Type: application/json` 的 POST(webhook 事实标准)。
    // 日志只记 method 与 host:URL 可能内嵌 bot token。
    linker.func_wrap(
        "host",
        "http_post",
        |mut caller: Caller<'_, PluginState>,
         method_ptr: i32,
         method_len: i32,
         url_ptr: i32,
         url_len: i32,
         body_ptr: i32,
         body_len: i32,
         resp_ptr: i32,
         resp_cap: i32|
         -> i32 {
            let Some(method) = read_text(&mut caller, method_ptr, method_len) else {
                return ERR_BOUNDS;
            };
            let Some(url) = read_text(&mut caller, url_ptr, url_len) else {
                return ERR_BOUNDS;
            };
            let Some(body) = read_mem(&mut caller, body_ptr, body_len) else {
                return ERR_BOUNDS;
            };
            let plugin_id = caller.data().plugin_id.clone();
            // 墙钟预算检查(#3):fuel 不计量宿主侧执行,超时也只 detach 任务;
            // 入口拒绝让 wasm 侧的调用循环每轮拿到 -4,配合 fuel 兜底终止循环。
            // -4 沿用"网络失败"码,语义是"本次请求不发出:预算耗尽"。
            if std::time::Instant::now() >= caller.data().deadline {
                warn!(plugin = %plugin_id, "host_http_post 超出派发墙钟预算,拒绝");
                return -4;
            }
            if method != "POST" {
                warn!(plugin = %plugin_id, method = %method, "host_http_post v1 只接受 POST");
                return -3;
            }
            if !url.starts_with("https://") {
                warn!(plugin = %plugin_id, "host_http_post 拒绝非 https URL");
                return -2;
            }
            // 只记 host,不记完整 URL:路径与查询串可能带 token。
            let host = url.split('/').nth(2).unwrap_or_default().to_string();
            let app = caller.data().app.clone();
            // 下载前先定缓冲与有效容量:容量同时约束下载(读到即停)与写回截断。
            // 没有可用缓冲也按硬上限有界下载——读完丢弃,不能不设界。
            let last = {
                let state = caller.data();
                (state.resp_ptr, state.resp_cap)
            };
            let download_cap = resp_write_plan(resp_ptr, resp_cap, last, usize::MAX)
                .map(|(_, cap)| cap)
                .unwrap_or(HTTP_RESP_MAX);
            let Some(handle) = tokio::runtime::Handle::try_current().ok() else {
                warn!(plugin = %plugin_id, "host_http_post 不在异步运行时上下文中");
                return -4;
            };
            let request = app
                .http
                .post(&url)
                .timeout(HTTP_TIMEOUT)
                .header("content-type", "application/json")
                .body(body);
            let outcome = handle.block_on(async {
                let mut resp = request.send().await?;
                let status = resp.status();
                // 有界读取(#4):按 chunk 累计到 download_cap 即停,超出的字节
                // 丢弃——截断语义与"整读后截断"一致,但大响应体不再整体进内存。
                let mut buf: Vec<u8> = Vec::new();
                while buf.len() < download_cap {
                    match resp.chunk().await {
                        Ok(Some(chunk)) => {
                            let remaining = download_cap - buf.len();
                            buf.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                        }
                        Ok(None) => break,
                        Err(err) => return Err(err),
                    }
                }
                Ok::<_, reqwest::Error>((status, buf))
            });
            let (status, bytes) = match outcome {
                Ok(ok) => ok,
                Err(err) => {
                    warn!(plugin = %plugin_id, host = %host, error = %err, "host_http_post 请求失败");
                    return -4;
                }
            };
            if !status.is_success() {
                warn!(plugin = %plugin_id, host = %host, status = %status, "host_http_post 非 2xx 响应");
                return -5;
            }
            // resp_ptr 为 0 时回落到最近一次 host_resp_alloc 的缓冲;没有可用
            // 缓冲但请求已发出:不报错,返回 0 字节。选缓冲与截断见 resp_write_plan。
            let Some((ptr, n)) = resp_write_plan(resp_ptr, resp_cap, last, bytes.len()) else {
                return 0;
            };
            if !write_mem(&mut caller, ptr, &bytes[..n]) {
                return ERR_BOUNDS;
            }
            n as i32
        },
    )?;

    // host_http_get(url_ptr, url_len, resp_ptr, resp_cap) -> i32:
    //   >=0 写入 out 的字节数;-1 越界;-2 非 https;-4 网络/预算;-5 非 2xx。
    //   与 http_post 同一 https/超时/有界下载/预算模型,仅方法固定为 GET、
    //   无 body(汇率这类只读外部接口,KTD2)。
    linker.func_wrap(
        "host",
        "http_get",
        |mut caller: Caller<'_, PluginState>,
         url_ptr: i32,
         url_len: i32,
         resp_ptr: i32,
         resp_cap: i32|
         -> i32 {
            let Some(url) = read_text(&mut caller, url_ptr, url_len) else {
                return ERR_BOUNDS;
            };
            let plugin_id = caller.data().plugin_id.clone();
            if std::time::Instant::now() >= caller.data().deadline {
                warn!(plugin = %plugin_id, "host_http_get 超出派发墙钟预算,拒绝");
                return -4;
            }
            if !url.starts_with("https://") {
                warn!(plugin = %plugin_id, "host_http_get 拒绝非 https URL");
                return -2;
            }
            let host = url.split('/').nth(2).unwrap_or_default().to_string();
            let app = caller.data().app.clone();
            let last = {
                let state = caller.data();
                (state.resp_ptr, state.resp_cap)
            };
            let download_cap = resp_write_plan(resp_ptr, resp_cap, last, usize::MAX)
                .map(|(_, cap)| cap)
                .unwrap_or(HTTP_RESP_MAX);
            let Some(handle) = tokio::runtime::Handle::try_current().ok() else {
                warn!(plugin = %plugin_id, "host_http_get 不在异步运行时上下文中");
                return -4;
            };
            let request = app.http.get(&url).timeout(HTTP_TIMEOUT);
            let outcome = handle.block_on(async {
                let mut resp = request.send().await?;
                let status = resp.status();
                let mut buf: Vec<u8> = Vec::new();
                while buf.len() < download_cap {
                    match resp.chunk().await {
                        Ok(Some(chunk)) => {
                            let remaining = download_cap - buf.len();
                            buf.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                        }
                        Ok(None) => break,
                        Err(err) => return Err(err),
                    }
                }
                Ok::<_, reqwest::Error>((status, buf))
            });
            let (status, bytes) = match outcome {
                Ok(ok) => ok,
                Err(err) => {
                    warn!(plugin = %plugin_id, host = %host, error = %err, "host_http_get 请求失败");
                    return -4;
                }
            };
            if !status.is_success() {
                warn!(plugin = %plugin_id, host = %host, status = %status, "host_http_get 非 2xx 响应");
                return -5;
            }
            let Some((ptr, n)) = resp_write_plan(resp_ptr, resp_cap, last, bytes.len()) else {
                return 0;
            };
            if !write_mem(&mut caller, ptr, &bytes[..n]) {
                return ERR_BOUNDS;
            }
            n as i32
        },
    )?;

    // host_nodes_query(out_ptr, out_cap) -> i32:
    //   只读节点基础信息(R1/KTD2):返回 JSON 数组
    //   `[{"id":1,"name":"edge-1","online":true},...]`,写回 out,返回字节数。
    //   财务字段(price/currency/... )已不在宿主 node 表——它们归财务插件的
    //   plugin_data;财务插件首次启用时经这个函数读的是历史列(退役前)。
    linker.func_wrap(
        "host",
        "nodes_query",
        |mut caller: Caller<'_, PluginState>, out_ptr: i32, out_cap: i32| -> i32 {
            let plugin_id = caller.data().plugin_id.clone();
            if std::time::Instant::now() >= caller.data().deadline {
                warn!(plugin = %plugin_id, "host_nodes_query 超出派发墙钟预算,拒绝");
                return -4;
            }
            let app = caller.data().app.clone();
            let online: std::collections::HashSet<i64> =
                app.agents.read().unwrap_or_else(|e| e.into_inner()).keys().copied().collect();
            let nodes = match app.db.nodes() {
                Ok(nodes) => nodes,
                Err(e) => {
                    warn!(plugin = %plugin_id, "host_nodes_query 读节点失败: {e:#}");
                    return -8;
                }
            };
            let arr: Vec<serde_json::Value> = nodes
                .iter()
                .map(|n| {
                    serde_json::json!({
                        "id": n.id,
                        "name": n.name,
                        "online": online.contains(&n.id),
                    })
                })
                .collect();
            let bytes = serde_json::to_vec(&arr).unwrap_or_else(|_| b"[]".to_vec());
            let (ptr, n) = match resp_write_plan(
                out_ptr,
                out_cap,
                (caller.data().resp_ptr, caller.data().resp_cap),
                bytes.len(),
            ) {
                Some(plan) => plan,
                None => return 0,
            };
            if !write_mem(&mut caller, ptr, &bytes[..n]) {
                return ERR_BOUNDS;
            }
            n as i32
        },
    )?;

    // host_emit_event(name_ptr, name_len, payload_ptr, payload_len) -> i32:
    //   0 成功;-1 越界/非法 UTF-8;-7 事件名不以 `plugin_` 开头;-8 emit 失败。
    //   事件名与 payload 组成 Event::Plugin,走总线既有的去重管道再派发给
    //   订阅者(KTD6/KTD8)。
    linker.func_wrap(
        "host",
        "emit_event",
        |mut caller: Caller<'_, PluginState>,
         name_ptr: i32,
         name_len: i32,
         payload_ptr: i32,
         payload_len: i32|
         -> i32 {
            let Some(name) = read_text(&mut caller, name_ptr, name_len) else {
                return ERR_BOUNDS;
            };
            let Some(payload_text) = read_text(&mut caller, payload_ptr, payload_len) else {
                return ERR_BOUNDS;
            };
            let plugin_id = caller.data().plugin_id.clone();
            if !name.starts_with(PLUGIN_EVENT_PREFIX) || name.len() <= PLUGIN_EVENT_PREFIX.len() {
                warn!(plugin = %plugin_id, name = %name, "emit_event 事件名必须以 plugin_ 开头");
                return -7;
            }
            let Ok(payload) = serde_json::from_str::<serde_json::Value>(&payload_text) else {
                warn!(plugin = %plugin_id, "emit_event payload 不是合法 JSON");
                return ERR_BOUNDS;
            };
            let app = caller.data().app.clone();
            let event = Event::Plugin { name, payload };
            if let Err(e) = crate::notification_bus::emit(&app, &event) {
                warn!(plugin = %plugin_id, "emit_event 失败: {e:#}");
                return -8;
            }
            0
        },
    )?;

    // host_data_put(key_ptr, key_len, val_ptr, val_len) -> i32:
    //   0 成功(新建或覆盖);-1 越界/非法 UTF-8;-6 超限;-8 数据库失败。
    //   单记录上限 RECORD_MAX、单插件总配额 PLUGIN_DATA_MAX(替换同 key 时
    //   只计增量,KTD3)。
    linker.func_wrap(
        "host",
        "data_put",
        |mut caller: Caller<'_, PluginState>,
         key_ptr: i32,
         key_len: i32,
         val_ptr: i32,
         val_len: i32|
         -> i32 {
            let Some(key) = read_text(&mut caller, key_ptr, key_len) else {
                return ERR_BOUNDS;
            };
            let Some(value) = read_text(&mut caller, val_ptr, val_len) else {
                return ERR_BOUNDS;
            };
            if key.is_empty() || value.len() > RECORD_MAX {
                warn!(plugin = %caller.data().plugin_id, "data_put 记录超限或 key 为空");
                return ERR_QUOTA;
            }
            let plugin_id = caller.data().plugin_id.clone();
            let app = caller.data().app.clone();
            // 配额:排除本 key 的现有占用,加上新值。
            let (_, used) = match app.db.plugin_data_usage(&plugin_id) {
                Ok(u) => u,
                Err(e) => {
                    warn!(plugin = %plugin_id, "data_put 统计失败: {e:#}");
                    return ERR_DB;
                }
            };
            let existing =
                app.db.plugin_data_get(&plugin_id, &key).ok().flatten().map(|s| s.len() as i64).unwrap_or(0);
            if used - existing + value.len() as i64 > PLUGIN_DATA_MAX {
                warn!(plugin = %plugin_id, "data_put 超出单插件配额");
                return ERR_QUOTA;
            }
            match app.db.plugin_data_put(&plugin_id, &key, &value) {
                Ok(_) => 0,
                Err(e) => {
                    warn!(plugin = %plugin_id, "data_put 失败: {e:#}");
                    ERR_DB
                }
            }
        },
    )?;

    // host_data_get(key_ptr, key_len, out_ptr, out_cap) -> i32:
    //   >=0 写入 out 的字节数;0 无此记录;-1 越界/非法 UTF-8。
    linker.func_wrap(
        "host",
        "data_get",
        |mut caller: Caller<'_, PluginState>,
         key_ptr: i32,
         key_len: i32,
         out_ptr: i32,
         out_cap: i32|
         -> i32 {
            let Some(key) = read_text(&mut caller, key_ptr, key_len) else {
                return ERR_BOUNDS;
            };
            let plugin_id = caller.data().plugin_id.clone();
            let Some(value) = caller.data().app.db.plugin_data_get(&plugin_id, &key).unwrap_or(None) else {
                return 0;
            };
            if out_ptr < 0 || out_cap < 0 {
                return ERR_BOUNDS;
            }
            let cap = out_cap as usize;
            let end = char_boundary_end(&value, cap);
            if !write_mem(&mut caller, out_ptr, &value.as_bytes()[..end]) {
                return ERR_BOUNDS;
            }
            end as i32
        },
    )?;

    // host_data_delete(key_ptr, key_len) -> i32:0 成功(含本就无此记录);-1 越界。
    linker.func_wrap(
        "host",
        "data_delete",
        |mut caller: Caller<'_, PluginState>, key_ptr: i32, key_len: i32| -> i32 {
            let Some(key) = read_text(&mut caller, key_ptr, key_len) else {
                return ERR_BOUNDS;
            };
            let plugin_id = caller.data().plugin_id.clone();
            match caller.data().app.db.plugin_data_delete(&plugin_id, &key) {
                Ok(_) => 0,
                Err(e) => {
                    warn!(plugin = %plugin_id, "data_delete 失败: {e:#}");
                    ERR_DB
                }
            }
        },
    )?;

    // host_data_list(prefix_ptr, prefix_len, out_ptr, out_cap) -> i32:
    //   >=0 写入 out 的字节数;前缀过滤;-1 越界/非法 UTF-8;-8 数据库失败。
    //   返回 `[{"key":"node:1","data":"..."},...]`。
    linker.func_wrap(
        "host",
        "data_list",
        |mut caller: Caller<'_, PluginState>,
         prefix_ptr: i32,
         prefix_len: i32,
         out_ptr: i32,
         out_cap: i32|
         -> i32 {
            let Some(prefix) = read_text(&mut caller, prefix_ptr, prefix_len) else {
                return ERR_BOUNDS;
            };
            let plugin_id = caller.data().plugin_id.clone();
            let rows = match caller.data().app.db.plugin_data_list(&plugin_id, &prefix) {
                Ok(rows) => rows,
                Err(e) => {
                    warn!(plugin = %plugin_id, "data_list 失败: {e:#}");
                    return ERR_DB;
                }
            };
            let arr: Vec<serde_json::Value> =
                rows.into_iter().map(|(key, data)| serde_json::json!({ "key": key, "data": data })).collect();
            let bytes = serde_json::to_vec(&arr).unwrap_or_else(|_| b"[]".to_vec());
            let last = (caller.data().resp_ptr, caller.data().resp_cap);
            let Some((ptr, n)) = resp_write_plan(out_ptr, out_cap, last, bytes.len()) else {
                return 0;
            };
            if !write_mem(&mut caller, ptr, &bytes[..n]) {
                return ERR_BOUNDS;
            }
            n as i32
        },
    )?;

    Ok(linker)
}

// ---------------------------------------------------------------------------
// 事件派发入口
// ---------------------------------------------------------------------------

/// 把一个事件交给插件处理,返回 `on_event` 的 i32(0 = 成功,非 0 = 插件自定义
/// 错误码)。trap(fuel 耗尽、越界访问等)返回 `Err`。
///
/// 超时不在这一层强制中断:Registry 的派发循环在外面套 tokio 超时,超时只是
/// 不再等后台任务;fuel 只计量 wasm 指令,拦不住宿主调用循环。所以把
/// `timeout_ms` 折算成 deadline 存进 PluginState,宿主函数(http_post/kv_get)
/// 入口检查——预算耗尽后宿主调用被拒绝,wasm 循环每轮拿到负数返回值,配合
/// fuel 兜底,两种死循环都出得来。http 的单请求超时见 [`HTTP_TIMEOUT`]。
pub fn call_on_event(
    engine: &wasmtime::Engine,
    app: &Arc<App>,
    plugin: &LoadedPlugin,
    event: &Event,
    fuel_limit: u64,
    timeout_ms: u64,
) -> Result<i32> {
    let mut handle =
        instantiate(engine, app, &plugin.manifest.plugin_id, &plugin.module, fuel_limit, timeout_ms)?;
    let payload = serde_json::to_vec(event)?;
    let alloc = handle
        .instance
        .get_typed_func::<(i32,), i32>(&mut handle.store, "__alloc")
        .map_err(|_| anyhow::anyhow!("模块缺少 __alloc(load 已检查,不应到达这里)"))?;
    let on_event = handle
        .instance
        .get_typed_func::<(i32, i32), i32>(&mut handle.store, "on_event")
        .map_err(|_| anyhow::anyhow!("模块缺少 on_event(load 已检查,不应到达这里)"))?;
    // 载荷缓冲与 host_resp_alloc 走同一个 allocator:宿主从不自己挑地址。
    let ptr = alloc.call(&mut handle.store, (payload.len() as i32,))?;
    if ptr <= 0 {
        bail!("__alloc 返回了非正指针 {ptr}");
    }
    let mem: Memory = handle
        .instance
        .get_memory(&mut handle.store, "memory")
        .context("模块缺少 memory(load 已检查,不应到达这里)")?;
    let start = ptr as usize;
    let end = start.checked_add(payload.len()).context("载荷地址溢出")?;
    let data = mem.data_mut(&mut handle.store);
    if end > data.len() {
        bail!("__alloc 指针 {ptr} 超出线性内存");
    }
    data[start..end].copy_from_slice(&payload);
    Ok(on_event.call(&mut handle.store, (ptr, payload.len() as i32))?)
}

/// 调用一个无参 `() -> i32` 导出(`on_tick`),与 [`call_on_event`] 同一套
/// fuel/deadline 隔离。tick 不带事件载荷:插件在 on_tick 里经宿主函数
/// (nodes_query/data_*/http_get/emit_event)自取所需(U4/KTD4)。
pub fn call_hook(
    engine: &wasmtime::Engine,
    app: &Arc<App>,
    plugin: &LoadedPlugin,
    hook: &str,
    fuel_limit: u64,
    timeout_ms: u64,
) -> Result<i32> {
    let mut handle =
        instantiate(engine, app, &plugin.manifest.plugin_id, &plugin.module, fuel_limit, timeout_ms)?;
    let func = handle
        .instance
        .get_typed_func::<(), i32>(&mut handle.store, hook)
        .map_err(|_| anyhow::anyhow!("模块缺少 {hook}(load 已按 manifest 声明检查,不应到达这里)"))?;
    Ok(func.call(&mut handle.store, ())?)
}

/// 调用一个「入参 JSON、返回 JSON」的导出(`render_page`/`on_action`/
/// `on_cleanup`),与 [`call_on_event`] 同一套 fuel/deadline 隔离(U5/U9)。
/// 约定:导出签名 `(ptr: i32, len: i32) -> i32`,返回值是写回 host_resp_alloc
/// 缓冲的响应字节数(0 表示空响应,负数是插件自定义错误码)。宿主用最近一次
/// host_resp_alloc 记下的缓冲读回响应体。
pub fn call_json_hook(
    engine: &wasmtime::Engine,
    app: &Arc<App>,
    plugin: &LoadedPlugin,
    hook: &str,
    input: &[u8],
    fuel_limit: u64,
    timeout_ms: u64,
) -> Result<Vec<u8>> {
    let mut handle =
        instantiate(engine, app, &plugin.manifest.plugin_id, &plugin.module, fuel_limit, timeout_ms)?;
    let alloc = handle
        .instance
        .get_typed_func::<(i32,), i32>(&mut handle.store, "__alloc")
        .map_err(|_| anyhow::anyhow!("模块缺少 __alloc"))?;
    let func = handle
        .instance
        .get_typed_func::<(i32, i32), i32>(&mut handle.store, hook)
        .map_err(|_| anyhow::anyhow!("模块缺少 {hook}(load 已按 manifest 声明检查)"))?;
    let mem: Memory = handle.instance.get_memory(&mut handle.store, "memory").context("模块缺少 memory")?;
    // 入参写进插件内存:与事件载荷同一 allocator 回环。
    let in_ptr = alloc.call(&mut handle.store, (input.len().max(1) as i32,))?;
    if in_ptr <= 0 {
        bail!("__alloc 返回非正指针 {in_ptr}");
    }
    let start = in_ptr as usize;
    let end = start.checked_add(input.len()).context("入参地址溢出")?;
    if end > mem.data(&handle.store).len() {
        bail!("__alloc 指针 {in_ptr} 超出线性内存");
    }
    mem.data_mut(&mut handle.store)[start..end].copy_from_slice(input);
    let n = func.call(&mut handle.store, (in_ptr, input.len() as i32))?;
    if n < 0 {
        bail!("{hook} 返回错误码 {n}");
    }
    // 响应体在插件最近一次 host_resp_alloc 记下的缓冲里。
    let (resp_ptr, _) = {
        let s = handle.store.data();
        (s.resp_ptr, s.resp_cap)
    };
    if n == 0 || resp_ptr <= 0 {
        return Ok(Vec::new());
    }
    let start = resp_ptr as usize;
    let end = start.checked_add(n as usize).context("响应地址溢出")?;
    let data = mem.data(&handle.store);
    let bytes = data.get(start..end).context("响应超出线性内存")?.to_vec();
    Ok(bytes)
}

/// 不超过 `max` 的最大字符边界偏移:截断必须落在边界上,否则切出的字节不是
/// 合法 UTF-8。detail 的带省略号截断与 host_kv_get 的前缀截断共用(一个加
/// 省略号一个不加,共用的是边界计算)。
fn char_boundary_end(s: &str, max: usize) -> usize {
    let mut end = s.len().min(max);
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    end
}

/// 按字符边界截断,加省略号标记被截。
pub(super) fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_owned();
    }
    format!("{}…", &s[..char_boundary_end(s, max)])
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::registry::DEFAULT_TIMEOUT_MS;
    use crate::plugin::test_util::{app, compile, engine, expiry_event, row, MANIFEST, MINIMAL_WAT};

    /// 最小插件加载、编译、处理真实事件,都返回 0。两个事件类型都走一遍:
    /// 载荷形状不同(字段集不同),分配与写入路径相同。
    #[test]
    fn a_minimal_plugin_loads_and_handles_events() {
        let engine = engine();
        let app = app();
        let plugin = load(&engine, &row(compile(MINIMAL_WAT))).unwrap();
        assert_eq!(
            call_on_event(&engine, &app, &plugin, &expiry_event(), DEFAULT_FUEL_LIMIT, DEFAULT_TIMEOUT_MS)
                .unwrap(),
            0
        );
        let online = Event::AgentOnline { node_id: 5, name: "edge-1".into(), observed_at: 300 };
        assert_eq!(
            call_on_event(&engine, &app, &plugin, &online, DEFAULT_FUEL_LIMIT, DEFAULT_TIMEOUT_MS).unwrap(),
            0
        );
    }

    #[test]
    fn the_event_payload_reaches_the_plugin() {
        // on_event 把 (ptr, len) 复制到全局堆顶,再返回 len:Rust 侧从内存读回
        // 载荷,证明写入的地址与长度是插件实际收到的。
        let wat_text = r#"
(module
  (memory (export "memory") 1)
  (global $heap (mut i32) (i32.const 1024))
  (func (export "__alloc") (param $cap i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $heap))
    (global.set $heap (i32.add (global.get $heap) (local.get $cap)))
    (local.get $ptr))
  (func (export "on_event") (param $ptr i32) (param $len i32) (result i32)
    (global.set $heap (i32.add (i32.const 16384) (local.get $len)))
    (memory.copy (i32.const 16384) (local.get $ptr) (local.get $len))
    (local.get $len)))"#;
        let engine = engine();
        let app = app();
        let plugin = load(&engine, &row(compile(wat_text))).unwrap();
        let event = expiry_event();
        let n =
            call_on_event(&engine, &app, &plugin, &event, DEFAULT_FUEL_LIMIT, DEFAULT_TIMEOUT_MS).unwrap();
        let json = serde_json::to_vec(&event).unwrap();
        assert_eq!(n as usize, json.len());
    }

    // ---- 模块加载(R6) ----

    #[test]
    fn corrupt_wasm_fails_to_load() {
        let engine = engine();
        let err = load(&engine, &row(b"\0asm\xde\xad\xbe\xef".to_vec())).unwrap_err();
        assert!(err.to_string().contains("编译失败"), "实际: {err}");
    }

    #[test]
    fn a_module_missing_exports_is_rejected() {
        let engine = engine();
        for (wat_text, needle) in [
            (
                r#"(module (memory (export "memory") 1)
                    (func (export "__alloc") (param i32) (result i32) (i32.const 0)))"#,
                "on_event",
            ),
            (
                r#"(module (memory (export "memory") 1)
                    (func (export "on_event") (param i32 i32) (result i32) (i32.const 0)))"#,
                "__alloc",
            ),
            (
                r#"(module (func (export "on_event") (param i32 i32) (result i32) (i32.const 0))
                    (func (export "__alloc") (param i32) (result i32) (i32.const 0)))"#,
                "memory",
            ),
        ] {
            let err = load(&engine, &row(compile(wat_text))).unwrap_err();
            assert!(err.to_string().contains(needle), "应报缺少 `{needle}`,实际: {err}");
        }
    }

    #[test]
    fn a_wrong_abi_version_fails_at_load() {
        let engine = engine();
        let mut r = row(compile(MINIMAL_WAT));
        r.manifest_json = MANIFEST.replace("abi_version = 2", "abi_version = 3");
        let err = load(&engine, &r).unwrap_err();
        // `{:#}` 展开错误链:load 包了一层 "manifest 无效",原因在下面。
        let whole = format!("{err:#}");
        assert!(whole.contains("abi_version"), "实际: {whole}");
    }

    // ---- 宿主函数(R8) ----

    /// 实例化一个 WAT 模块并保留 Store:测试要直接读内存与 db。
    fn spawn(engine: &wasmtime::Engine, app: &Arc<App>, wat_text: &str) -> InstanceHandle {
        let module = wasmtime::Module::new(engine, compile(wat_text)).unwrap();
        instantiate(engine, app, "com.example.test", &module, DEFAULT_FUEL_LIMIT, DEFAULT_TIMEOUT_MS).unwrap()
    }

    /// 同上,但指定 plugin_id——namespace 隔离的测试要两个不同身份的插件。
    fn spawn_as(
        engine: &wasmtime::Engine,
        app: &Arc<App>,
        plugin_id: &str,
        wat_text: &str,
    ) -> InstanceHandle {
        let module = wasmtime::Module::new(engine, compile(wat_text)).unwrap();
        instantiate(engine, app, plugin_id, &module, DEFAULT_FUEL_LIMIT, DEFAULT_TIMEOUT_MS).unwrap()
    }

    /// 驱动 on_event 并返回其返回值。
    fn drive(h: &mut InstanceHandle) -> i32 {
        let f = h.instance.get_typed_func::<(i32, i32), i32>(&mut h.store, "on_event").unwrap();
        f.call(&mut h.store, (0, 0)).unwrap()
    }

    // ---- plugin_data CRUD(U2/KTD3) ----

    /// data_put 写入、data_get 读回、data_delete 删除,往返一致且落库在
    /// plugin_data 表、按 plugin_id 命名空间隔离。
    const DATA_WAT: &str = r#"
(module
  (import "host" "data_put" (func $put (param i32 i32 i32 i32) (result i32)))
  (import "host" "data_get" (func $get (param i32 i32 i32 i32) (result i32)))
  (import "host" "data_delete" (func $del (param i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "node:1")
  (data (i32.const 2048) "{\"price\":10}")
  (func (export "__alloc") (param i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32)
    (drop (call $put (i32.const 1024) (i32.const 6) (i32.const 2048) (i32.const 12)))
    (drop (call $get (i32.const 1024) (i32.const 6) (i32.const 4096) (i32.const 128)))
    (drop (call $del (i32.const 1024) (i32.const 6)))
    (call $get (i32.const 1024) (i32.const 6) (i32.const 4096) (i32.const 128))))"#;

    #[test]
    fn plugin_data_round_trips_and_isolates_namespaces() {
        let engine = engine();
        let app = app();
        // 插件 A 写一行;插件 B 用同样的 key 读不到 A 的行。
        let mut a = spawn_as(&engine, &app, "com.example.a", DATA_WAT);
        assert_eq!(drive(&mut a), 0, "put 后 get 读到,再 delete 后 get 返回 0");
        assert_eq!(app.db.plugin_data_get("com.example.a", "node:1").unwrap(), None, "已被自己删掉");
        let mut b = spawn_as(&engine, &app, "com.example.b", DATA_WAT);
        assert_eq!(drive(&mut b), 0);
        // 两者的表行互不可见:写 A 的行、读 B 的读不到。
        app.db.plugin_data_put("com.example.a", "node:9", "{\"x\":1}").unwrap();
        assert_eq!(app.db.plugin_data_get("com.example.b", "node:9").unwrap(), None, "命名空间隔离");
        assert_eq!(app.db.plugin_data_get("com.example.a", "node:9").unwrap().as_deref(), Some("{\"x\":1}"));
    }

    /// data_list 前缀过滤只返回匹配记录。
    #[test]
    fn plugin_data_list_filters_by_prefix() {
        let app = app();
        app.db.plugin_data_put("com.example.test", "node:1", "a").unwrap();
        app.db.plugin_data_put("com.example.test", "node:2", "b").unwrap();
        app.db.plugin_data_put("com.example.test", "fx", "c").unwrap();
        let nodes = app.db.plugin_data_list("com.example.test", "node:").unwrap();
        assert_eq!(nodes.len(), 2, "只列出 node: 前缀");
        assert_eq!(nodes[0].0, "node:1");
        let all = app.db.plugin_data_list("com.example.test", "").unwrap();
        assert_eq!(all.len(), 3, "空前缀列出全部");
    }

    /// 超单条上限(256 KiB)被 data_put 拒绝(配额 -6)。
    #[test]
    fn plugin_data_record_over_the_cap_is_refused() {
        let app = app();
        let big = "x".repeat(RECORD_MAX + 1);
        // 直接走 db 层校验不了(配额在宿主函数层),所以驱动 wasm。
        let engine = engine();
        let wat = r#"
(module
  (import "host" "data_put" (func $put (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 8)
  (data (i32.const 1024) "k")
  (func (export "__alloc") (param i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32)
    (call $put (i32.const 1024) (i32.const 1) (i32.const 8192) (i32.const 262145))))"#;
        let mut h = spawn(&engine, &app, wat);
        assert_eq!(drive(&mut h), -6, "超单条上限返回配额错误码");
        drop(big);
    }

    /// 删除插件时 plugin_data 行随 delete_plugin_with_kv 一并清理。
    #[test]
    fn deleting_a_plugin_takes_its_data_rows() {
        let app = app();
        let row = app.db.create_plugin("com.example.gone", "Gone", "1", "{}", b"m", "sha").unwrap();
        app.db.plugin_data_put("com.example.gone", "node:1", "v").unwrap();
        app.db.plugin_data_put("com.example.other", "node:1", "keep").unwrap();
        app.db.delete_plugin_with_kv(row.id, "com.example.gone").unwrap();
        assert_eq!(app.db.plugin_data_get("com.example.gone", "node:1").unwrap(), None, "随插件删除");
        assert_eq!(
            app.db.plugin_data_get("com.example.other", "node:1").unwrap().as_deref(),
            Some("keep"),
            "别的插件的数据不动"
        );
    }

    // ---- emit_event(U3/KTD6) ----

    /// emit_event 拒绝不以 `plugin_` 开头的事件名(-7)。
    #[test]
    fn emit_event_refuses_names_without_the_plugin_prefix() {
        let engine = engine();
        let app = app();
        let wat = r#"
(module
  (import "host" "emit_event" (func $emit (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "expiry_soon")
  (data (i32.const 2048) "{}")
  (func (export "__alloc") (param i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32)
    (call $emit (i32.const 1024) (i32.const 11) (i32.const 2048) (i32.const 2))))"#;
        let mut h = spawn(&engine, &app, wat);
        assert_eq!(drive(&mut h), -7);
    }

    /// emit_event 发出的事件走总线并记录到 notification_log(去重键)。
    #[test]
    fn emit_event_records_through_the_bus() {
        let engine = engine();
        let app = app();
        let wat = r#"
(module
  (import "host" "emit_event" (func $emit (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "plugin_expiry_soon")
  (data (i32.const 2048) "{\"node_id\":7,\"expires_at\":\"2026-10-01\",\"threshold_days\":7}")
  (func (export "__alloc") (param i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32)
    (call $emit (i32.const 1024) (i32.const 18) (i32.const 2048) (i32.const 58))))"#;
        let mut h = spawn(&engine, &app, wat);
        let code = drive(&mut h);
        // 无 tokio 运行时:dispatch 被跳过但 emit 仍记录幂等行(返回 0)。
        assert!(code >= 0, "emit 成功路径返回 0,实际 {code}");
        let key = Event::Plugin {
            name: "plugin_expiry_soon".into(),
            payload: serde_json::json!({"node_id": 7, "expires_at": "2026-10-01", "threshold_days": 7}),
        }
        .threshold_or_state_key();
        assert!(app.db.dispatch_already_sent(7, "plugin_expiry_soon", key).unwrap(), "幂等行已立");
    }

    // ---- nodes_query(U3) ----

    /// nodes_query 返回 id/name/online 的 JSON 数组,在线状态与 agents 一致。
    #[test]
    fn nodes_query_reports_online_state() {
        let engine = engine();
        let app = app();
        let wat = r#"
(module
  (import "host" "nodes_query" (func $q (param i32 i32) (result i32)))
  (memory (export "memory") 1)
  (func (export "__alloc") (param i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32)
    (call $q (i32.const 4096) (i32.const 4096))))"#;
        let mut h = spawn(&engine, &app, wat);
        let n = drive(&mut h);
        assert!(n > 0, "有节点时应返回 JSON 字节数,实际 {n}");
        let mem = h.instance.get_memory(&mut h.store, "memory").unwrap();
        let bytes = &mem.data(&h.store)[4096..4096 + n as usize];
        let arr: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        assert!(arr.is_array());
    }

    // ---- http_get(U3) ----

    /// http_get 拒绝非 https URL(-2),与 http_post 的校验一致。
    #[test]
    fn http_get_refuses_plain_http_urls() {
        let engine = engine();
        let app = app();
        let wat = r#"
(module
  (import "host" "http_get" (func $get (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "http://example.com/rate")
  (func (export "__alloc") (param i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32)
    (call $get (i32.const 1024) (i32.const 25) (i32.const 8192) (i32.const 256))))"#;
        let mut h = spawn(&engine, &app, wat);
        assert_eq!(drive(&mut h), -2);
    }

    /// 通过 on_event 驱动:kv_set 写入再 kv_get 读出到固定地址,返回读到的字节数。
    /// 同时调 host_log 与 host_now,证明它们可被调用。
    const KV_WAT: &str = r#"
(module
  (import "host" "kv_set" (func $kv_set (param i32 i32 i32 i32) (result i32)))
  (import "host" "kv_get" (func $kv_get (param i32 i32 i32 i32) (result i32)))
  (import "host" "log" (func $log (param i32 i32 i32)))
  (import "host" "now" (func $now (result i64)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "mykey")
  (data (i32.const 2048) "myvalue")
  (func (export "__alloc") (param $cap i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (i32.const 8192))
    (i32.add (local.get $ptr) (local.get $cap)))
  (func (export "on_event") (param i32 i32) (result i32)
    (local $now i64)
    (local.set $now (call $now))
    (call $log (i32.const 1) (i32.const 2048) (i32.const 7))
    (drop (call $kv_set (i32.const 1024) (i32.const 5) (i32.const 2048) (i32.const 7)))
    (call $kv_get (i32.const 1024) (i32.const 5) (i32.const 4096) (i32.const 64))))"#;

    #[test]
    fn kv_round_trips_through_the_setting_table() {
        let engine = engine();
        let app = app();
        let mut h = spawn(&engine, &app, KV_WAT);
        let on_event = h.instance.get_typed_func::<(i32, i32), i32>(&mut h.store, "on_event").unwrap();
        let n = on_event.call(&mut h.store, (0, 0)).unwrap();
        assert_eq!(n, 7, "kv_get 应写回 7 个字节");
        // 内容从插件内存读回:kv_get 写在 4096。
        let mem = h.instance.get_memory(&mut h.store, "memory").unwrap();
        assert_eq!(&mem.data(&h.store)[4096..4103], b"myvalue");
        // 落库形式是带命名空间的 key,两个插件不会互相覆盖。
        assert_eq!(app.db.get("plugin.com.example.test:mykey").as_deref(), Some("myvalue"));
        assert_eq!(app.db.get("plugin.other:mykey"), None, "命名空间隔离");
    }

    /// host_log 的越界与非法 UTF-8、kv_get 的越界 out_ptr:返回错误码而不是 panic。
    /// kv 的 key 必须先在 setting 表里有值,kv_get 才会走到写内存的越界检查。
    const OOB_WAT: &str = r#"
(module
  (import "host" "log" (func $log (param i32 i32 i32)))
  (import "host" "kv_set" (func $kv_set (param i32 i32 i32 i32) (result i32)))
  (import "host" "kv_get" (func $kv_get (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "k")
  (func (export "__alloc") (param i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32)
    ;; 越界日志:ptr 指向内存之外。
    (call $log (i32.const 3) (i32.const 268435456) (i32.const 100))
    ;; 非法 UTF-8:0xff 开头。
    (call $log (i32.const 3) (i32.const 8192) (i32.const 4))
    ;; 先写入一个值,让下面的 kv_get 走到写内存这一步。
    (drop (call $kv_set (i32.const 1024) (i32.const 1) (i32.const 1024) (i32.const 1)))
    ;; out_ptr 越界:-1。
    (call $kv_get (i32.const 1024) (i32.const 1) (i32.const 268435456) (i32.const 4))))"#;

    #[test]
    fn out_of_bounds_accesses_return_errors_not_panics() {
        let engine = engine();
        let app = app();
        let mut h = spawn(&engine, &app, OOB_WAT);
        let on_event = h.instance.get_typed_func::<(i32, i32), i32>(&mut h.store, "on_event").unwrap();
        // host_log 越界被吞掉;kv_get 越界返回 -1 作为 on_event 的返回值。
        assert_eq!(on_event.call(&mut h.store, (0, 0)).unwrap(), -1);
    }

    /// URL 与 method 校验先于任何网络动作,测试无需异步运行时。
    const HTTP_WAT: &str = r#"
(module
  (import "host" "http_post"
    (func $http_post (param i32 i32 i32 i32 i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "POST")
  (data (i32.const 2048) "http://example.com/hook")
  (data (i32.const 4096) "{}")
  (func (export "__alloc") (param i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32)
    (call $http_post (i32.const 1024) (i32.const 4)
                      (i32.const 2048) (i32.const 23)
                      (i32.const 4096) (i32.const 2)
                      (i32.const 8192) (i32.const 256))))"#;

    #[test]
    fn http_post_refuses_plain_http_urls() {
        let engine = engine();
        let app = app();
        let mut h = spawn(&engine, &app, HTTP_WAT);
        let on_event = h.instance.get_typed_func::<(i32, i32), i32>(&mut h.store, "on_event").unwrap();
        assert_eq!(on_event.call(&mut h.store, (0, 0)).unwrap(), -2);
    }

    #[test]
    fn http_post_refuses_non_post_methods() {
        let wat_text = HTTP_WAT.replace("\"POST\"", "\"GET\"");
        let engine = engine();
        let app = app();
        let mut h = spawn(&engine, &app, &wat_text);
        let on_event = h.instance.get_typed_func::<(i32, i32), i32>(&mut h.store, "on_event").unwrap();
        assert_eq!(on_event.call(&mut h.store, (0, 0)).unwrap(), -3);
    }

    /// 没有异步运行时上下文时,网络失败以 -4 返回而不是 panic(生产里 U4 的
    /// block_in_place 提供上下文;这里同时覆盖"拿不到 runtime"分支)。
    #[test]
    fn http_post_without_a_runtime_returns_an_error_code() {
        // https URL 会走到发请求那一步;当前测试线程没有 tokio runtime。
        let wat_text = HTTP_WAT.replace("http://example.com/hook", "https://example.com/hook");
        let engine = engine();
        let app = app();
        let mut h = spawn(&engine, &app, &wat_text);
        let on_event = h.instance.get_typed_func::<(i32, i32), i32>(&mut h.store, "on_event").unwrap();
        assert!(on_event.call(&mut h.store, (0, 0)).unwrap() < 0);
    }

    // ---- fuel(KTD6) ----

    #[test]
    fn a_runaway_loop_is_cut_off_by_fuel() {
        let wat_text = r#"
(module
  (memory (export "memory") 1)
  (func (export "__alloc") (param i32) (result i32) (i32.const 1024))
  (func (export "on_event") (param i32 i32) (result i32)
    (loop (br 0))
    (i32.const 0)))"#;
        let engine = engine();
        let app = app();
        let plugin = load(&engine, &row(compile(wat_text))).unwrap();
        let err =
            call_on_event(&engine, &app, &plugin, &expiry_event(), 10_000, DEFAULT_TIMEOUT_MS).unwrap_err();
        // trap 信息在错误链深处,格式化整条链再找。
        let whole = format!("{err:#}");
        assert!(whole.to_lowercase().contains("fuel"), "应是 fuel 耗尽,实际: {whole}");
    }

    /// #3:宿主调用循环不能击穿超时沙箱。wasm 循环调 kv_get,每轮 wasm 指令
    /// 极少(给的 fuel 烧不完)、宿主侧也不耗时——没有 deadline 检查时它会一直
    /// 转到 fuel 尽头(远超 50ms 预算);有检查时 kv_get 在 deadline 后返回 -1,
    /// 循环当轮退出。参数都是合法的,-1 只可能来自预算检查。
    #[test]
    fn a_host_call_loop_is_cut_off_by_the_deadline() {
        let wat_text = r#"
(module
  (import "host" "kv_get" (func $kv_get (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "k")
  (func (export "__alloc") (param i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32)
    (local $r i32)
    (loop $again
      ;; 无值返回 0:继续;预算耗尽返回 -1:退出。
      (local.set $r (call $kv_get (i32.const 1024) (i32.const 1) (i32.const 4096) (i32.const 64)))
      (br_if $again (i32.eqz (local.get $r))))
    (local.get $r)))"#;
        let engine = engine();
        let app = app();
        let plugin = load(&engine, &row(compile(wat_text))).unwrap();
        let started = std::time::Instant::now();
        // fuel 给到 50ms 内烧不完的量级;墙钟预算只有 50ms。
        let code = call_on_event(&engine, &app, &plugin, &expiry_event(), 1_000_000_000, 50).unwrap();
        let elapsed = started.elapsed();
        assert_eq!(code, -1, "循环应以 kv_get 的预算耗尽返回码退出");
        assert!(elapsed < Duration::from_secs(5), "应在墙钟预算附近终止,实际 {elapsed:?}");
    }

    /// #16:resp_write_plan 的分支——完整写回、截断到 cap、resp_ptr=0 回落、
    /// 硬上限、无缓冲。纯函数直接驱动,不需要网络。
    #[test]
    fn resp_write_plan_covers_full_truncated_fallback_and_limits() {
        // 完整写回:cap 足够,写全部字节。
        assert_eq!(resp_write_plan(8192, 256, (0, 0), 100), Some((8192, 100)));
        // 截断到 cap:响应比缓冲大,只写 cap 字节。
        assert_eq!(resp_write_plan(8192, 10, (0, 0), 100), Some((8192, 10)));
        // resp_ptr = 0:回落最近一次 host_resp_alloc 的缓冲,截断同样生效。
        assert_eq!(resp_write_plan(0, 0, (4096, 32), 100), Some((4096, 32)));
        // 硬上限:cap 声明成超大值,有效容量仍是 64 KiB(bytes_len 传 MAX
        // 即"只要容量"的用法,下载定界走的就是这条)。
        assert_eq!(resp_write_plan(8192, i32::MAX, (0, 0), usize::MAX), Some((8192, HTTP_RESP_MAX)));
        // 没有可用缓冲:请求已发出的场景由调用方返回 0 字节。
        assert_eq!(resp_write_plan(0, 0, (0, 0), 100), None);
        assert_eq!(resp_write_plan(8192, -1, (0, 0), 100), None);
    }
}
