//! 13 个宿主函数的契约测试:用 `ContractState`(内存后端)真 instantiate 一个
//! 导入这些函数的 wasm 模块,逐个驱动并断言返回值/内存写回/错误码。
//!
//! 这些断言是"契约"本身:monitor 主 crate 里同一份闭包换 `PluginState` 跑,行为
//! 必须一致(monitor 的 `plugin::host_funcs` 测试覆盖那一侧)。

use std::time::{Duration, Instant};

use monitor_plugin_contract::constants::{KV_VALUE_MAX, PLUGIN_DATA_MAX};
use monitor_plugin_contract::error_codes::{ERR_BOUNDS, ERR_DB, ERR_QUOTA, ERR_SSRF};
use monitor_plugin_contract::{
    linker, setting_key, ContractState, HasScratch, Host, HttpMethod, MockHttp, NoopHttp,
};

const PLUGIN_ID: &str = "com.example.test";

fn engine() -> wasmtime::Engine {
    let mut config = wasmtime::Config::new();
    // 与 monitor 的 `plugin::new_engine` 同配置:不开 fuel 的话 Store::set_fuel
    // 静默无效,死循环插件不会被中断(契约测试给的是充裕额度,只求配置一致)。
    config.consume_fuel(true);
    wasmtime::Engine::new(&config).unwrap()
}

/// 所有用例共用的模块骨架:`on_event` 的**函数体**由 `body` 给出(要自平衡),
/// 函数与模块的闭合括号由模板补。
fn module(body: &str, data: &str, pages: u32) -> String {
    format!(
        r#"(module
  {imports}
  (memory (export "memory") {pages})
  {data}
  (func (export "__alloc") (param i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32)
    {body}
  )
)
"#,
        imports = IMPORTS,
        pages = pages,
        data = data,
        body = body,
    )
}

/// 13 个宿主函数的 import 声明,每个用例按需取用(WAT 里没被调到的 import 也
/// 会被链接,这是有意的:instantiate 成功本身就是"13 个函数签名齐全"的断言)。
const IMPORTS: &str = r#"
  (import "host" "log" (func $log (param i32 i32 i32)))
  (import "host" "now" (func $now (result i64)))
  (import "host" "kv_get" (func $kv_get (param i32 i32 i32 i32) (result i32)))
  (import "host" "kv_set" (func $kv_set (param i32 i32 i32 i32) (result i32)))
  (import "host" "resp_alloc" (func $resp_alloc (param i32) (result i32)))
  (import "host" "http_post" (func $http_post (param i32 i32 i32 i32 i32 i32 i32 i32) (result i32)))
  (import "host" "http_get" (func $http_get (param i32 i32 i32 i32) (result i32)))
  (import "host" "nodes_query" (func $nodes_query (param i32 i32) (result i32)))
  (import "host" "emit_event" (func $emit (param i32 i32 i32 i32) (result i32)))
  (import "host" "data_put" (func $data_put (param i32 i32 i32 i32) (result i32)))
  (import "host" "data_get" (func $data_get (param i32 i32 i32 i32) (result i32)))
  (import "host" "data_delete" (func $data_delete (param i32 i32) (result i32)))
  (import "host" "data_list" (func $data_list (param i32 i32 i32 i32) (result i32)))"#;

/// 把一段文本折成 WAT 字符串字面量里的转义形式(`"` 与 `\` 要转义,否则
/// `{"price":10}` 这类 JSON 会把 data 段提前截断)。
fn wat_str(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// 编译 WAT、建 Store 并 instantiate 一遍真 linker。
fn spawn(state: ContractState, wat_text: &str) -> (wasmtime::Store<ContractState>, wasmtime::Instance) {
    let engine = engine();
    let module = wasmtime::Module::new(&engine, wat::parse_str(wat_text).unwrap()).unwrap();
    let mut store = wasmtime::Store::new(&engine, state);
    // 同 monitor 的 `instantiate`:执行前先给 fuel,否则一执行就 trap。
    store.set_fuel(1_000_000_000).unwrap();
    let instance = linker::<ContractState>(&engine).unwrap().instantiate(&mut store, &module).unwrap();
    (store, instance)
}

/// 驱动 `on_event()`(不带载荷:这些用例只考宿主函数)。
fn drive(store: &mut wasmtime::Store<ContractState>, instance: &wasmtime::Instance) -> i32 {
    let f = instance.get_typed_func::<(i32, i32), i32>(&mut *store, "on_event").unwrap();
    f.call(store, (0, 0)).unwrap()
}

fn memory(store: &mut wasmtime::Store<ContractState>, instance: &wasmtime::Instance) -> Vec<u8> {
    let mem = instance.get_memory(&mut *store, "memory").unwrap();
    mem.data(&*store).to_vec()
}

/// 把 `MockHttp` 放进 `Box<dyn ContractHttp>`,同时留一个句柄读它的 calls。
struct MockHttpProxy(std::sync::Arc<MockHttp>);

impl monitor_plugin_contract::ContractHttp for MockHttpProxy {
    fn request(
        &self,
        method: HttpMethod,
        url: &str,
        body: Option<Vec<u8>>,
        deadline: Instant,
    ) -> Result<Vec<u8>, i32> {
        self.0.request(method, url, body, deadline)
    }
}

fn mocked(mock: &std::sync::Arc<MockHttp>) -> ContractState {
    ContractState::for_test(PLUGIN_ID).with_http(Box::new(MockHttpProxy(mock.clone())))
}

// ---------------------------------------------------------------------------
// host_now / host_log:不碰宿主数据,只用 scratch
// ---------------------------------------------------------------------------

/// `now` 是当前 Unix 秒(用一个下界判,别把系统时间钉死);`log` 落进汇集点。
#[test]
fn now_and_log_use_only_the_scratch() {
    let wat = module(
        "(call $log (i32.const 2) (i32.const 1024) (i32.const 12))
    (i64.gt_s (call $now) (i64.const 1600000000))",
        r#"(data (i32.const 1024) "no bot_token")"#,
        1,
    );
    let state = ContractState::for_test(PLUGIN_ID);
    state.push_log("宿主侧预置"); // 汇集点是调用方持有的,预置的一条要留在原位。
    let (mut store, instance) = spawn(state, &wat);
    assert_eq!(drive(&mut store, &instance), 1, "now 应返回当前 Unix 秒");
    let state = store.into_data();
    assert_eq!(state.logs(), vec!["宿主侧预置".to_owned(), "no bot_token".to_owned()]);
}

/// `log` 越界/非法 UTF-8 不该 panic,也不该落进汇集点(只有宿主日志)。
#[test]
fn log_swallows_out_of_bounds_and_invalid_utf8() {
    let wat = module(
        "(call $log (i32.const 3) (i32.const 268435456) (i32.const 100))
    (call $log (i32.const 3) (i32.const 8192) (i32.const 2))
    (i32.const 0)",
        r#"(data (i32.const 8192) "\ff\ff")"#,
        1,
    );
    let (mut store, instance) = spawn(ContractState::for_test(PLUGIN_ID), &wat);
    assert_eq!(drive(&mut store, &instance), 0);
    assert_eq!(store.into_data().logs(), Vec::<String>::new());
}

// ---------------------------------------------------------------------------
// kv_get / kv_set
// ---------------------------------------------------------------------------

/// kv 往返:值按命名空间落库、读回的字节数与内容都对。
#[test]
fn kv_round_trips_under_the_plugin_namespace() {
    let value = "中文值";
    let wat = module(
        &format!(
            "(drop (call $kv_set (i32.const 1024) (i32.const 5) (i32.const 2048) (i32.const {})))
    (call $kv_get (i32.const 1024) (i32.const 5) (i32.const 4096) (i32.const 64))",
            value.len()
        ),
        r#"(data (i32.const 1024) "mykey")
  (data (i32.const 2048) "中文值")"#,
        1,
    );
    let (mut store, instance) = spawn(ContractState::for_test(PLUGIN_ID), &wat);
    assert_eq!(drive(&mut store, &instance), value.len() as i32);
    let mem = memory(&mut store, &instance);
    assert_eq!(&mem[4096..4096 + value.len()], value.as_bytes());
    // 落库形式是带命名空间的 key:两个插件不会互相覆盖。
    let state = store.into_data();
    assert_eq!(state.kv().try_get(&setting_key(PLUGIN_ID, "mykey")).as_deref(), Some(value));
    assert_eq!(state.kv().try_get(&setting_key("com.example.other", "mykey")), None, "命名空间隔离");
}

/// kv_get 的 out_cap 只放得下半个字符时,写回的是合法 UTF-8 前缀。
#[test]
fn kv_get_truncates_on_a_character_boundary() {
    let wat = module(
        "(drop (call $kv_set (i32.const 1024) (i32.const 5) (i32.const 2048) (i32.const 9)))
    (call $kv_get (i32.const 1024) (i32.const 5) (i32.const 4096) (i32.const 4))",
        r#"(data (i32.const 1024) "mykey")
  (data (i32.const 2048) "中文字")"#,
        1,
    );
    let (mut store, instance) = spawn(ContractState::for_test(PLUGIN_ID), &wat);
    assert_eq!(drive(&mut store, &instance), 3, "4 字节放不下一个 3 字节的汉字加一个");
    assert_eq!(&memory(&mut store, &instance)[4096..4099], "中".as_bytes());
}

/// 值超 `KV_VALUE_MAX` 返回 -1;无值返回 0;out_ptr 越界返回 -1。
#[test]
fn kv_errors_follow_the_documented_codes() {
    let over = module(
        &format!(
            "(call $kv_set (i32.const 1024) (i32.const 1) (i32.const 0) (i32.const {}))",
            KV_VALUE_MAX + 1
        ),
        r#"(data (i32.const 1024) "k")"#,
        2,
    );
    let (mut store, instance) = spawn(ContractState::for_test(PLUGIN_ID), &over);
    assert_eq!(drive(&mut store, &instance), ERR_BOUNDS, "值超上限 -1");

    // 无值:0(不是错误)。
    let missing = module(
        "(call $kv_get (i32.const 1024) (i32.const 1) (i32.const 4096) (i32.const 64))",
        r#"(data (i32.const 1024) "k")"#,
        1,
    );
    let (mut store, instance) = spawn(ContractState::for_test(PLUGIN_ID), &missing);
    assert_eq!(drive(&mut store, &instance), 0);

    // 有值但 out_ptr 越界:-1。
    let oob = module(
        "(drop (call $kv_set (i32.const 1024) (i32.const 1) (i32.const 1024) (i32.const 1)))
    (call $kv_get (i32.const 1024) (i32.const 1) (i32.const 268435456) (i32.const 4))",
        r#"(data (i32.const 1024) "k")"#,
        1,
    );
    let (mut store, instance) = spawn(ContractState::for_test(PLUGIN_ID), &oob);
    assert_eq!(drive(&mut store, &instance), ERR_BOUNDS);
}

/// 读库失败给 -8 而不是 0:0 的含义是「无值或空」,把库故障并进去,插件就分不出
/// 「没配」与「库坏了」。
#[test]
fn kv_get_reports_a_storage_error_instead_of_no_value() {
    /// 一个所有操作都失败的宿主:验证 `Err(())` 折成 -8 而不是并进「无值」。
    struct BrokenHost;
    impl Host for BrokenHost {
        fn kv_get(&self, _key: &str) -> Result<Option<String>, ()> {
            Err(())
        }
        fn kv_set(&self, _key: &str, _value: &str) -> Result<(), ()> {
            Err(())
        }
        fn data_put(&self, _p: &str, _k: &str, _v: &str) -> Result<bool, ()> {
            Err(())
        }
        fn data_get(&self, _p: &str, _k: &str) -> Result<Option<String>, ()> {
            Err(())
        }
        fn data_delete(&self, _p: &str, _k: &str) -> Result<(), ()> {
            Err(())
        }
        fn data_list(&self, _p: &str, _x: &str) -> Result<Vec<(String, String)>, ()> {
            Err(())
        }
        fn nodes_query(&self) -> Result<Vec<monitor_plugin_contract::NodeInfo>, ()> {
            Err(())
        }
        fn emit_event(&self, _name: &str, _payload: serde_json::Value) -> Result<(), ()> {
            Err(())
        }
        fn http_request(
            &self,
            _m: HttpMethod,
            _u: &str,
            _b: Option<Vec<u8>>,
            _cap: usize,
            _d: Instant,
        ) -> Result<Vec<u8>, i32> {
            Err(ERR_SSRF)
        }
    }
    impl HasScratch for BrokenHost {
        fn plugin_id(&self) -> &str {
            PLUGIN_ID
        }
        fn deadline(&self) -> Instant {
            Instant::now() + Duration::from_secs(60)
        }
        fn resp(&self) -> (i32, i32) {
            (0, 0)
        }
        fn set_resp(&mut self, _ptr: i32, _cap: i32) {}
        fn push_log(&self, _text: &str) {}
    }

    let engine = engine();
    let wat = module(
        "(call $kv_get (i32.const 1024) (i32.const 1) (i32.const 4096) (i32.const 64))",
        r#"(data (i32.const 1024) "k")"#,
        1,
    );
    let module = wasmtime::Module::new(&engine, wat::parse_str(&wat).unwrap()).unwrap();
    let mut store = wasmtime::Store::new(&engine, BrokenHost);
    store.set_fuel(1_000_000_000).unwrap();
    let instance = linker::<BrokenHost>(&engine).unwrap().instantiate(&mut store, &module).unwrap();
    let f = instance.get_typed_func::<(i32, i32), i32>(&mut store, "on_event").unwrap();
    assert_eq!(f.call(&mut store, (0, 0)).unwrap(), ERR_DB, "库故障给 -8,不能并进「无值」");
}

// ---------------------------------------------------------------------------
// resp_alloc / http_post / http_get
// ---------------------------------------------------------------------------

/// resp_alloc 走 `__alloc` 回环并记进 scratch;http_post 用显式 resp_ptr=0 回落到
/// 那个缓冲;MockHttp 收到的是 POST + `{}`。
#[test]
fn http_post_writes_the_response_into_the_resp_buffer() {
    let url = "https://example.com/hook";
    let wat = module(
        &format!(
            "(drop (call $resp_alloc (i32.const 256)))
    (call $http_post (i32.const 1024) (i32.const 4)
                     (i32.const 2048) (i32.const {})
                     (i32.const 4096) (i32.const 2)
                     (i32.const 0) (i32.const 0))",
            url.len()
        ),
        &format!(
            r#"(data (i32.const 1024) "POST")
  (data (i32.const 2048) "{url}")
  (data (i32.const 4096) "{{}}")"#
        ),
        1,
    );
    let mock = std::sync::Arc::new(MockHttp::respond_with(200, "hello"));
    let (mut store, instance) = spawn(mocked(&mock), &wat);
    assert_eq!(drive(&mut store, &instance), 5);
    assert_eq!(store.data().resp(), (8192, 256), "resp_alloc 的缓冲记进了 scratch");
    assert_eq!(&memory(&mut store, &instance)[8192..8197], b"hello");
    assert_eq!(
        mock.calls(),
        vec![monitor_plugin_contract::HttpCall {
            method: HttpMethod::Post,
            url: url.into(),
            body: Some(b"{}".to_vec()),
        }]
    );
}

/// 下载按缓冲容量收窄:MockHttp 回 1000 字节,插件的缓冲只有 16 字节,写回的是
/// 16 字节(截断语义与真宿主的有界下载一致)。
#[test]
fn http_download_is_bounded_by_the_declared_capacity() {
    let url = "https://example.com/rate";
    let wat = module(
        &format!(
            "(call $http_get (i32.const 1024) (i32.const {}) (i32.const 4096) (i32.const 16))",
            url.len()
        ),
        &format!(r#"(data (i32.const 1024) "{url}")"#),
        1,
    );
    let response = "x".repeat(1000);
    let mock = std::sync::Arc::new(MockHttp::respond_with(200, response.clone()));
    let (mut store, instance) = spawn(mocked(&mock), &wat);
    assert_eq!(drive(&mut store, &instance), 16);
    assert_eq!(&memory(&mut store, &instance)[4096..4112], &response.as_bytes()[..16]);
    assert_eq!(mock.calls().len(), 1);
}

/// 非 2xx 是 -5;没装 http 后端(默认 NoopHttp)一律按 SSRF 拒绝码 -9 拒。
#[test]
fn http_failures_follow_the_documented_codes() {
    let url = "https://example.com/internal";
    let wat = module(
        &format!(
            "(call $http_get (i32.const 1024) (i32.const {}) (i32.const 4096) (i32.const 64))",
            url.len()
        ),
        &format!(r#"(data (i32.const 1024) "{url}")"#),
        1,
    );

    let mock = std::sync::Arc::new(MockHttp::respond_with(500, "boom"));
    let (mut store, instance) = spawn(mocked(&mock), &wat);
    assert_eq!(drive(&mut store, &instance), -5, "非 2xx");

    let (mut store, instance) = spawn(ContractState::for_test(PLUGIN_ID), &wat);
    assert_eq!(drive(&mut store, &instance), ERR_SSRF, "默认 NoopHttp 全拒,与真宿主的 SSRF 码一致");

    let state = ContractState::for_test(PLUGIN_ID).with_http(Box::new(NoopHttp));
    let (mut store, instance) = spawn(state, &wat);
    assert_eq!(drive(&mut store, &instance), ERR_SSRF);
}

/// 非 https 与(http_post 的)非 POST method 在发请求之前就拒掉。
#[test]
fn http_refuses_plain_http_and_non_post_before_sending() {
    let plain = "http://example.com/hook";
    let wat = module(
        &format!(
            "(call $http_get (i32.const 1024) (i32.const {}) (i32.const 4096) (i32.const 64))",
            plain.len()
        ),
        &format!(r#"(data (i32.const 1024) "{plain}")"#),
        1,
    );
    let mock = std::sync::Arc::new(MockHttp::respond_with(200, ""));
    let (mut store, instance) = spawn(mocked(&mock), &wat);
    assert_eq!(drive(&mut store, &instance), -2);
    assert!(mock.calls().is_empty(), "被拒的请求不该出网");

    let method_wat = module(
        "(call $http_post (i32.const 1024) (i32.const 3)
                        (i32.const 2048) (i32.const 21)
                        (i32.const 4096) (i32.const 2)
                        (i32.const 8192) (i32.const 64))",
        r#"(data (i32.const 1024) "GET")
  (data (i32.const 2048) "https://example.com/x")
  (data (i32.const 4096) "{}")"#,
        1,
    );
    let mock = std::sync::Arc::new(MockHttp::respond_with(200, ""));
    let (mut store, instance) = spawn(mocked(&mock), &method_wat);
    assert_eq!(drive(&mut store, &instance), -3, "v1 只接受 POST");
    assert!(mock.calls().is_empty());
}

// ---------------------------------------------------------------------------
// nodes_query
// ---------------------------------------------------------------------------

/// nodes_query 返回 id/name/online/created_at 的 JSON 数组,在线状态与在线集合
/// 一致。`created_at` 是插件的机器身份(宿主 id 会被 SQLite 复用),它必须随
/// 每一行回到 guest 缓冲里——那是这个函数的 ABI 面之一。
#[test]
fn nodes_query_reports_online_state() {
    let wat = module("(call $nodes_query (i32.const 4096) (i32.const 4096))", "", 1);
    let state = ContractState::for_test(PLUGIN_ID);
    state.nodes().add_node(1, "edge-up", 1_700_000_001);
    state.nodes().add_node(2, "edge-down", 1_700_000_002);
    state.nodes().set_online(1, true);
    let (mut store, instance) = spawn(state, &wat);
    let n = drive(&mut store, &instance);
    assert!(n > 0, "有节点时应返回 JSON 字节数,实际 {n}");
    let mem = memory(&mut store, &instance);
    let arr: serde_json::Value = serde_json::from_slice(&mem[4096..4096 + n as usize]).unwrap();
    assert_eq!(
        arr,
        serde_json::json!([
            {"id": 1, "name": "edge-up", "online": true, "created_at": 1_700_000_001},
            {"id": 2, "name": "edge-down", "online": false, "created_at": 1_700_000_002},
        ])
    );
}

/// 空节点表回 `[]`。
#[test]
fn nodes_query_returns_an_empty_array_when_there_are_no_nodes() {
    let wat = module("(call $nodes_query (i32.const 4096) (i32.const 4096))", "", 1);
    let (mut store, instance) = spawn(ContractState::for_test(PLUGIN_ID), &wat);
    assert_eq!(drive(&mut store, &instance), 2);
    assert_eq!(&memory(&mut store, &instance)[4096..4098], b"[]");
}

// ---------------------------------------------------------------------------
// emit_event
// ---------------------------------------------------------------------------

/// 事件名必须以 `plugin_` 开头(-7);合法事件被投递(替身记下来);payload 不是
/// 合法 JSON 是 -1。
#[test]
fn emit_event_requires_the_plugin_prefix() {
    let bad = module(
        "(call $emit (i32.const 1024) (i32.const 11) (i32.const 2048) (i32.const 2))",
        r#"(data (i32.const 1024) "expiry_soon")
  (data (i32.const 2048) "{}")"#,
        1,
    );
    let (mut store, instance) = spawn(ContractState::for_test(PLUGIN_ID), &bad);
    assert_eq!(drive(&mut store, &instance), -7);
    assert!(store.into_data().events().is_empty(), "被拒的事件不该投递");

    let payload = r#"{"node_id":7}"#;
    let good = module(
        &format!(
            "(call $emit (i32.const 1024) (i32.const 18) (i32.const 2048) (i32.const {}))",
            payload.len()
        ),
        &format!(
            r#"(data (i32.const 1024) "plugin_expiry_soon")
  (data (i32.const 2048) "{}")"#,
            wat_str(payload)
        ),
        1,
    );
    let (mut store, instance) = spawn(ContractState::for_test(PLUGIN_ID), &good);
    assert_eq!(drive(&mut store, &instance), 0);
    assert_eq!(
        store.into_data().events(),
        vec![("plugin_expiry_soon".to_owned(), serde_json::json!({"node_id": 7}))]
    );

    let bad_json = module(
        "(call $emit (i32.const 1024) (i32.const 18) (i32.const 2048) (i32.const 2))",
        r#"(data (i32.const 1024) "plugin_expiry_soon")
  (data (i32.const 2048) "[}")"#,
        1,
    );
    let (mut store, instance) = spawn(ContractState::for_test(PLUGIN_ID), &bad_json);
    assert_eq!(drive(&mut store, &instance), ERR_BOUNDS);
}

// ---------------------------------------------------------------------------
// data_put / data_get / data_delete / data_list
// ---------------------------------------------------------------------------

/// plugin_data 往返:put 后 get 读到,delete 后 get 归 0(本就无此记录也算成功)。
#[test]
fn plugin_data_round_trips() {
    let value = r#"{"price":10}"#;
    let put_get = module(
        &format!(
            "(drop (call $data_put (i32.const 1024) (i32.const 6) (i32.const 2048) (i32.const {})))
    (call $data_get (i32.const 1024) (i32.const 6) (i32.const 4096) (i32.const 128))",
            value.len()
        ),
        &format!(
            r#"(data (i32.const 1024) "node:1")
  (data (i32.const 2048) "{}")"#,
            wat_str(value)
        ),
        1,
    );
    let (mut store, instance) = spawn(ContractState::for_test(PLUGIN_ID), &put_get);
    assert_eq!(drive(&mut store, &instance), value.len() as i32);
    assert_eq!(&memory(&mut store, &instance)[4096..4096 + value.len()], value.as_bytes());
    assert_eq!(store.data().kv().plugin_data_get(PLUGIN_ID, "node:1").as_deref(), Some(value));

    let del = module(
        "(drop (call $data_delete (i32.const 1024) (i32.const 6)))
    (call $data_get (i32.const 1024) (i32.const 6) (i32.const 4096) (i32.const 128))",
        r#"(data (i32.const 1024) "node:1")"#,
        1,
    );
    let (mut store, instance) = spawn(ContractState::for_test(PLUGIN_ID), &del);
    assert_eq!(drive(&mut store, &instance), 0, "无此记录:delete 成功、get 返回 0");
}

/// data_list 写回 `[{"key":...,"data":...}]`:前缀过滤、按 key 排序、插件隔离。
#[test]
fn data_list_returns_keys_and_data_sorted() {
    let wat = module(
        "(call $data_list (i32.const 1024) (i32.const 5) (i32.const 4096) (i32.const 1024))",
        r#"(data (i32.const 1024) "node:")"#,
        1,
    );
    let state = ContractState::for_test(PLUGIN_ID);
    state.kv().plugin_data_put_within_quota(PLUGIN_ID, "node:2", "b", PLUGIN_DATA_MAX);
    state.kv().plugin_data_put_within_quota(PLUGIN_ID, "node:1", "a", PLUGIN_DATA_MAX);
    state.kv().plugin_data_put_within_quota(PLUGIN_ID, "fx", "c", PLUGIN_DATA_MAX);
    state.kv().plugin_data_put_within_quota("com.example.other", "node:1", "keep", PLUGIN_DATA_MAX);
    let (mut store, instance) = spawn(state, &wat);
    let n = drive(&mut store, &instance);
    let mem = memory(&mut store, &instance);
    let arr: serde_json::Value = serde_json::from_slice(&mem[4096..4096 + n as usize]).unwrap();
    assert_eq!(
        arr,
        serde_json::json!([
            {"key": "node:1", "data": "a"},
            {"key": "node:2", "data": "b"},
        ])
    );
}

/// 空 key 与超单条上限(RECORD_MAX)都是 -6;总配额超限也是 -6(Host 层)。
#[test]
fn data_put_refuses_records_and_quotas_over_the_limit() {
    let empty_key = module(
        "(call $data_put (i32.const 1024) (i32.const 0) (i32.const 1024) (i32.const 0))",
        r#"(data (i32.const 1024) "k")"#,
        1,
    );
    let (mut store, instance) = spawn(ContractState::for_test(PLUGIN_ID), &empty_key);
    assert_eq!(drive(&mut store, &instance), ERR_QUOTA, "空 key");

    let over_record = module(
        &format!(
            "(call $data_put (i32.const 1024) (i32.const 1) (i32.const 8192) (i32.const {}))",
            256 * 1024 + 1
        ),
        r#"(data (i32.const 1024) "k")"#,
        8,
    );
    let (mut store, instance) = spawn(ContractState::for_test(PLUGIN_ID), &over_record);
    assert_eq!(drive(&mut store, &instance), ERR_QUOTA, "超 256 KiB 单条上限");

    // 总配额:直接走 Host 层,免得在 wasm 里搬 16 MiB。
    let state = ContractState::for_test(PLUGIN_ID);
    let big = "x".repeat(PLUGIN_DATA_MAX as usize + 1);
    assert_eq!(state.data_put(PLUGIN_ID, "k", &big), Ok(false), "超单插件总配额");
    assert_eq!(state.data_get(PLUGIN_ID, "k"), Ok(None), "被拒的写不留痕迹");
}

/// data_get 的 out_cap 只放得下半个字符时,写回的是合法 UTF-8 前缀。
#[test]
fn data_get_truncates_on_a_character_boundary() {
    let wat = module(
        "(call $data_get (i32.const 1024) (i32.const 1) (i32.const 4096) (i32.const 4))",
        r#"(data (i32.const 1024) "k")"#,
        1,
    );
    let state = ContractState::for_test(PLUGIN_ID);
    state.kv().plugin_data_put_within_quota(PLUGIN_ID, "k", "中文字", PLUGIN_DATA_MAX);
    let (mut store, instance) = spawn(state, &wat);
    assert_eq!(drive(&mut store, &instance), 3);
    assert_eq!(&memory(&mut store, &instance)[4096..4099], "中".as_bytes());
}

// ---------------------------------------------------------------------------
// 墙钟预算
// ---------------------------------------------------------------------------

/// 预算耗尽的截止点:kv_get -1、http_* 与 nodes_query -4(与真宿主一节的入口
/// 检查同一个行为)。
#[test]
fn an_expired_deadline_refuses_host_calls() {
    fn expired() -> ContractState {
        ContractState::for_test(PLUGIN_ID).with_deadline(Instant::now() - Duration::from_secs(1))
    }

    let kv = module(
        "(call $kv_get (i32.const 1024) (i32.const 1) (i32.const 4096) (i32.const 64))",
        r#"(data (i32.const 1024) "k")"#,
        1,
    );
    let (mut store, instance) = spawn(expired(), &kv);
    assert_eq!(drive(&mut store, &instance), ERR_BOUNDS);

    let nodes = module("(call $nodes_query (i32.const 4096) (i32.const 4096))", "", 1);
    let (mut store, instance) = spawn(expired(), &nodes);
    assert_eq!(drive(&mut store, &instance), -4);

    let url = "https://example.com/rate";
    let get = module(
        &format!(
            "(call $http_get (i32.const 1024) (i32.const {}) (i32.const 4096) (i32.const 64))",
            url.len()
        ),
        &format!(r#"(data (i32.const 1024) "{url}")"#),
        1,
    );
    let (mut store, instance) = spawn(expired(), &get);
    assert_eq!(drive(&mut store, &instance), -4);

    let post = module(
        "(call $http_post (i32.const 1024) (i32.const 4)
                        (i32.const 2048) (i32.const 21)
                        (i32.const 4096) (i32.const 2)
                        (i32.const 8192) (i32.const 64))",
        r#"(data (i32.const 1024) "POST")
  (data (i32.const 2048) "https://example.com/x")
  (data (i32.const 4096) "{}")"#,
        1,
    );
    let (mut store, instance) = spawn(expired(), &post);
    assert_eq!(drive(&mut store, &instance), -4);
}

/// 契约替身不是"`App` 的子集":构造只吃一个 `plugin_id`。
#[test]
fn for_test_needs_only_a_plugin_id() {
    let state = ContractState::for_test("com.example.only-an-id");
    assert_eq!(state.plugin_id(), "com.example.only-an-id");
    assert_eq!(state.kv().plugin_data_usage("com.example.only-an-id"), (0, 0));
}
