//! 宿主函数面的**真宿主一侧**:SSRF 预检、插件 http 的请求骨架,以及把 13 个
//! 宿主函数挂到 `PluginState` 上的 `host_linker`。
//!
//! 13 个宿主函数的闭包体在 `monitor-plugin-contract` 里——那才是唯一实现:一份
//! 闭包泛型在 `Host`/`HasScratch` 上,这里用真宿主 `PluginState` 实例化它,契约
//! 测试用 `ContractState` 实例化同一份闭包。留在本模块的只有**只有真宿主做得到**
//! 的两件事:
//!
//! - 插件 http 的 SSRF 预检与有界下载——它要 `App::plugin_http`(不跟随重定向的
//!   那个 client),契约 crate 不依赖 reqwest;
//! - 网段判定([`address_is_blocked`]),它是"插件能去哪"的策略,宿主专有。
//!
//! 每个函数对插件可见的签名、返回码与限额见契约 crate 里各函数的注释(错误码
//! 统一为负数,表见 `monitor_plugin_contract::error_codes`)。

use std::net::IpAddr;
use std::time::Duration;

use anyhow::Result;
use tracing::warn;
use wasmtime::Linker;

use crate::App;

use super::host::{PluginState, HTTP_TIMEOUT};
use monitor_plugin_contract::error_codes::ERR_SSRF;

/// 一个 IP 是否属于插件不该访问的网段:私有、回环、链路本地、云元数据
/// (169.254.169.254 落在 169.254/16)、CGNAT、基准测试与各类保留段。
///
/// 这一层只拦**插件发起**的请求。宿主自身的 http 调用(主题下载、GitHub、
/// 运维配置的 `github_proxy` 镜像)走 `App::http`,不受此限——把代理指向内网
/// 镜像是正当部署,一刀切会把它打断。插件则不同:它的 http 能力是"往公网
/// https 发请求",不该成为探内网、云元数据或回环服务的跳板。
fn address_is_blocked(addr: IpAddr) -> bool {
    match addr {
        IpAddr::V4(v4) => {
            let [a, b, c, _] = v4.octets();
            v4.is_private()        // 10/8, 172.16/12, 192.168/16
                || v4.is_loopback()   // 127/8
                || v4.is_link_local() // 169.254/16
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_documentation()
                || v4.is_multicast()
                || (a == 100 && (64..128).contains(&b)) // 100.64/10 CGNAT
                || (a == 192 && b == 0 && c == 0) // 192.0.0/24 IETF 保留
                || (a == 198 && (b == 18 || b == 19)) // 198.18/15 基准测试
                || a >= 240 // 240/4 保留(含 255.255.255.255)
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (v6.segments()[0] & 0xfe00) == 0xfc00 // fc00::/7 唯一本地
                || (v6.segments()[0] & 0xffc0) == 0xfe80 // fe80::/10 链路本地
                // ::ffff:a.b.c.d 这类映射地址按内嵌的 v4 判。
                || v6.to_ipv4_mapped().is_some_and(|v4| address_is_blocked(IpAddr::V4(v4)))
        }
    }
}

/// 检查一个插件 http 目标是否指向公网。`Err` 是立即返回给插件的错误码:URL
/// 解析不出主机名 → -2(与"非 https"同类,URL 形状不对);解析到的任一地址
/// 落在受限网段 → -9;解析失败或超预算 → -4(网络/超时,不是策略拒绝)。
///
/// `budget` 是本次调用的剩余墙钟预算。解析必须受它约束:`lookup_host` 是
/// `spawn_blocking` + 系统解析器,没有自己的超时,卡住时(常见 10-40 秒)会
/// 穿透派发预算——而它是**请求之前**的一步,任何挂在请求上的超时都管不到。
///
/// 先解析、再由 reqwest 自己再解析一次发送,中间留着一个 DNS rebinding 的
/// 时间窗。这里不追求把它彻底焊死:威胁模型是"管理员安装的插件",而真正
/// 的隔离来自 wasm 沙箱的其余边界;要焊死需要自定义 reqwest 的 Resolve 实现,
/// 代价与收益不成比例。
async fn http_target_is_allowed(url: &str, budget: Duration) -> Result<(), i32> {
    let parsed = reqwest::Url::parse(url).map_err(|_| -2)?;
    let Some(host) = parsed.host_str().filter(|h| !h.is_empty()) else {
        return Err(-2);
    };
    // `host_str` 对 IPv6 返回带方括号的形式(`[::1]`),剥掉才认得出是字面 IP。
    // 不剥的话它会掉进下面的 DNS 分支,判定结果随解析器而变。
    let bare = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')).unwrap_or(host);
    if let Ok(ip) = bare.parse::<IpAddr>() {
        return if address_is_blocked(ip) { Err(ERR_SSRF) } else { Ok(()) };
    }
    let port = parsed.port_or_known_default().unwrap_or(443);
    let Ok(resolved) = tokio::time::timeout(budget, tokio::net::lookup_host((bare, port))).await else {
        warn!(host = %bare, "SSRF 预检的 DNS 解析超出派发预算");
        return Err(-4);
    };
    for addr in resolved.map_err(|_| -4)? {
        if address_is_blocked(addr.ip()) {
            return Err(ERR_SSRF);
        }
    }
    Ok(())
}

/// 插件 http 的共同骨架:SSRF 预检 → 按剩余预算发一次请求 → 有界下载。
/// `http_post` 与 `http_get` 只差方法、请求体与日志词,其余(预检、预算收缩、
/// 重定向策略、有界读取、错误码映射)全在这里,不重复第二遍。
///
/// 返回 `Ok(响应体)` 或 `Err(错误码)`:预检拒绝用预检自己的码(-2/-9)、预检或
/// 请求超时/网络失败 -4、非 2xx -5。日志都在这里发,调用方只做内存写回。
#[allow(clippy::too_many_arguments)]
pub(super) async fn plugin_http_fetch(
    app: &App,
    plugin_id: &str,
    label: &str,
    method: reqwest::Method,
    url: &str,
    body: Option<Vec<u8>>,
    download_cap: usize,
    deadline: std::time::Instant,
) -> Result<Vec<u8>, i32> {
    // 只记 host,不记完整 URL:路径与查询串可能带 token。
    let host = url.split('/').nth(2).unwrap_or_default();
    // SSRF 预检。它的 DNS 解析按剩余墙钟预算收缩——`lookup_host` 没有自己的
    // 超时,卡住会穿透预算,而它是请求之前的一步。
    let budget = deadline.saturating_duration_since(std::time::Instant::now());
    if let Err(code) = http_target_is_allowed(url, budget).await {
        warn!(plugin = %plugin_id, host = %host, "{label} 拒绝私有/保留地址或预检超出预算");
        return Err(code);
    }
    // 请求超时按剩余预算收缩:固定 4 秒会让"预算只剩 1 秒"的调用在后台把请求
    // 跑完,预算就不是硬边界了。
    let left = deadline.saturating_duration_since(std::time::Instant::now()).min(HTTP_TIMEOUT);
    // 插件专用 client:不跟随重定向(见 App::plugin_http 的注释)。3xx 因此表现
    // 为非 2xx(-5),而不是被跟到预检没看过的目标。
    let mut request = app.plugin_http.request(method, url).timeout(left);
    if let Some(body) = body {
        request = request.header("content-type", "application/json").body(body);
    }
    let mut resp = match request.send().await {
        Ok(resp) => resp,
        Err(err) => {
            warn!(plugin = %plugin_id, host = %host, error = %err, "{label} 请求失败");
            return Err(-4);
        }
    };
    let status = resp.status();
    // 有界读取:按 chunk 累计到 download_cap 即停,超出的字节丢弃——截断语义
    // 与"整读后截断"一致,但大响应体不再整体进内存。
    let mut buf: Vec<u8> = Vec::new();
    while buf.len() < download_cap {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                let remaining = download_cap - buf.len();
                buf.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
            }
            Ok(None) => break,
            Err(err) => {
                warn!(plugin = %plugin_id, host = %host, error = %err, "{label} 读取响应失败");
                return Err(-4);
            }
        }
    }
    if !status.is_success() {
        warn!(plugin = %plugin_id, host = %host, status = %status, "{label} 非 2xx 响应");
        return Err(-5);
    }
    Ok(buf)
}

/// 注册 13 个宿主函数:唯一实现在契约 crate,这里把泛型参数绑成真宿主。
/// 每次实例化都重建:Linker 不能跨 Store 复用已定义的 Func,重建的开销微秒级,
/// 正确性优先。
pub(super) fn host_linker(engine: &wasmtime::Engine) -> Result<Linker<PluginState>> {
    monitor_plugin_contract::linker::<PluginState>(engine)
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use monitor_plugin_contract::constants::RECORD_MAX;
    use monitor_plugin_contract::error_codes::ERR_DB;

    use crate::notification_bus::Event;
    use crate::plugin::host::{instantiate, InstanceHandle, DEFAULT_FUEL_LIMIT};
    use crate::plugin::log::new_log_sink;
    use crate::plugin::registry::DEFAULT_TIMEOUT_MS;
    use crate::plugin::test_util::{app, compile, engine};

    // ---- 宿主函数(R8) ----

    /// 实例化一个 WAT 模块并保留 Store:测试要直接读内存与 db。
    fn spawn(engine: &wasmtime::Engine, app: &Arc<App>, wat_text: &str) -> InstanceHandle {
        let module = wasmtime::Module::new(engine, compile(wat_text)).unwrap();
        instantiate(
            engine,
            app,
            "com.example.test",
            &module,
            DEFAULT_FUEL_LIMIT,
            DEFAULT_TIMEOUT_MS,
            &new_log_sink(),
        )
        .unwrap()
    }

    /// 同上,但指定 plugin_id——namespace 隔离的测试要两个不同身份的插件。
    fn spawn_as(
        engine: &wasmtime::Engine,
        app: &Arc<App>,
        plugin_id: &str,
        wat_text: &str,
    ) -> InstanceHandle {
        let module = wasmtime::Module::new(engine, compile(wat_text)).unwrap();
        instantiate(engine, app, plugin_id, &module, DEFAULT_FUEL_LIMIT, DEFAULT_TIMEOUT_MS, &new_log_sink())
            .unwrap()
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

    /// nodes_query 返回 id/name/online/created_at 的 JSON 数组,在线状态与 agents
    /// 一致。`created_at` 是插件的机器身份(宿主 id 会被 SQLite 复用),它必须随
    /// 每一行回到 guest 缓冲里——那是这个函数的 ABI 面之一。
    #[test]
    fn nodes_query_reports_online_state() {
        let engine = engine();
        let app = app();
        // 播种两台:一台有 agent 会话(在线)、一台没有。不播种的话返回值是
        // "[]",旧断言"是个数组"会空过——测试名声称的在线语义从未被验证。
        let online = app
            .db
            .create_node(&crate::db::Node { name: "edge-up".into(), ..Default::default() }, "tok-up")
            .unwrap();
        let offline = app
            .db
            .create_node(&crate::db::Node { name: "edge-down".into(), ..Default::default() }, "tok-down")
            .unwrap();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        app.agents
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(online, crate::agent_ws::Agent::new(1, tx));

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
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 2, "{arr:?}");
        let by_id = |id: i64| arr.iter().find(|v| v["id"] == id).cloned().unwrap();
        assert_eq!(by_id(online)["name"], serde_json::json!("edge-up"));
        assert_eq!(by_id(online)["online"], serde_json::json!(true), "有会话的节点在线");
        assert_eq!(by_id(offline)["online"], serde_json::json!(false), "没有会话的节点离线");
        // 身份字段逐台与库里的值对齐:插件靠它认出「同一个 id 换了机器」。
        for id in [online, offline] {
            let from_db = app.db.node_identity(id).unwrap().unwrap().1;
            assert_eq!(
                by_id(id)["created_at"],
                serde_json::json!(from_db),
                "节点 {id} 的 created_at 要回给 guest"
            );
        }
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

    // ---- SSRF 防线(v2) ----

    /// 网段判定表:私有、回环、链路本地、云元数据、CGNAT、保留段都算受限;
    /// 真实公网地址不误伤。
    #[test]
    fn address_is_blocked_classifies_private_and_reserved_ranges() {
        for blocked in [
            "10.1.2.3",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1", // 私有
            "127.0.0.1",
            "127.9.9.9", // 回环
            "169.254.169.254",
            "169.254.0.1", // 链路本地 + 云元数据
            "0.0.0.0",
            "255.255.255.255", // 未指定 / 广播
            "100.64.0.1",
            "100.127.255.255", // CGNAT
            "192.0.0.1",
            "198.18.0.1",
            "240.0.0.1",
            "224.0.0.1", // 保留 / 基准 / 组播
            "::1",
            "::",
            "fc00::1",
            "fd12:3456::1",
            "fe80::1",
            "ff02::1", // v6 各类
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1", // v4 映射
        ] {
            let ip: IpAddr = blocked.parse().unwrap();
            assert!(address_is_blocked(ip), "{blocked} 应被判为受限");
        }
        for allowed in ["8.8.8.8", "1.1.1.1", "172.32.0.1", "100.63.255.255", "2606:4700::1111"] {
            let ip: IpAddr = allowed.parse().unwrap();
            assert!(!address_is_blocked(ip), "{allowed} 是公网地址,不该被拦");
        }
    }

    /// 策略层:字面 IP 与「主机名解析到私网」都拒。localhost 走 /etc/hosts,
    /// 不依赖外部 DNS。
    #[tokio::test]
    async fn http_target_is_allowed_refuses_private_hosts() {
        for url in [
            "https://127.0.0.1/hook",
            "https://169.254.169.254/latest/meta-data",
            "https://10.1.2.3/x",
            "https://192.168.1.1/x",
            "https://[::1]/x",
            "https://localhost/x",
        ] {
            assert_eq!(http_target_is_allowed(url, Duration::from_secs(5)).await, Err(ERR_SSRF), "{url}");
        }
        // 没有主机名的 URL 先一步按 -2 拒掉。scheme 校验不在这里:调用方
        // (http_get/http_post)已在进入前拦掉非 https,这里只看目标地址。
        // 注意 `https:///nohost` 会被 URL 解析器折叠成主机 `nohost`(要走
        // DNS),所以用 `https://` 这个必然缺主机名的形状。
        assert_eq!(http_target_is_allowed("https://", Duration::from_secs(5)).await, Err(-2));
    }

    /// 端到端:插件的 http_get 打到私网地址,宿主函数返回 -9 而不是发请求。
    /// 走真实的 spawn_blocking + block_on 路径,与生产里的调用方式一致。
    #[test]
    fn http_get_refuses_private_targets_end_to_end() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let engine = engine();
        let app = app();
        let url = "https://169.254.169.254/latest/meta-data";
        let wat = format!(
            r#"
(module
  (import "host" "http_get" (func $get (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 2)
  (data (i32.const 1024) "{url}")
  (func (export "__alloc") (param i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32)
    (call $get (i32.const 1024) (i32.const {len}) (i32.const 8192) (i32.const 256))))"#,
            len = url.len()
        );
        let code = rt.block_on(async move {
            tokio::task::spawn_blocking(move || {
                let mut h = spawn(&engine, &app, &wat);
                drive(&mut h)
            })
            .await
            .unwrap()
        });
        assert_eq!(code, ERR_SSRF, "云元数据地址必须被拒");
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

    /// 读库失败给 -8 而不是 0:0 的含义是「无值或空」,把库故障并进去,插件就分不出
    /// 「没配」与「库坏了」。旧插件把任何 <=0 都当无值,所以行为不变,只是现在能区分。
    #[test]
    fn kv_get_reports_a_database_error_instead_of_no_value() {
        let engine = engine();
        let app = app();
        app.db.set("plugin.com.example.test:mykey", "myvalue").unwrap();
        // 表没了 —— 一次真实的读库失败(而不是「这一行不在」)。
        app.db.conn().execute("DROP TABLE setting", []).unwrap();
        let mut h = spawn(&engine, &app, KV_WAT);
        assert_eq!(drive(&mut h), ERR_DB, "库坏了要给 -8,不能并进「无值」");
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
}
