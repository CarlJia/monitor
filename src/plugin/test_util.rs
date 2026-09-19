//! 测试共享的 fixture 与助手:manifest 文本、db 行、WAT 编译、App/Registry 装配。
//! api.rs 的测试与本目录各模块的测试共用同一份,fixture 漂移会让两边测的
//! 不是同一个东西。

use std::sync::Arc;

use crate::db::Db;
use crate::notification_bus::Event;
use crate::{db::PluginRow, App};

use super::host::new_engine;

/// 最小合法模块:memory + bump 分配器 + 恒返回 0 的 on_event。api.rs 的上传
/// 测试与本模块的测试共用同一份,fixture 漂移会让两边测的不是同一个东西。
pub(crate) const MINIMAL_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (global $heap (mut i32) (i32.const 1024))
  (func (export "__alloc") (param $cap i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $heap))
    (global.set $heap (i32.add (global.get $heap) (local.get $cap)))
    (local.get $ptr))
  (func (export "on_event") (param i32 i32) (result i32) (i32.const 0)))"#;

/// on_event 往自己的 kv 命名空间写 called=1:被派发没被派发,db 里见。
pub(crate) const KV_CALLED_WAT: &str = r#"
(module
  (import "host" "kv_set" (func $kv_set (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "called")
  (data (i32.const 2048) "1")
  (func (export "__alloc") (param $cap i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32)
    (drop (call $kv_set (i32.const 1024) (i32.const 6) (i32.const 2048) (i32.const 1)))
    (i32.const 0)))"#;

/// on_tick 往自己的 kv 命名空间写 called=1:被 tick 没被 tick,db 里见。
pub(crate) const KV_TICK_WAT: &str = r#"
(module
  (import "host" "kv_set" (func $kv_set (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "called")
  (data (i32.const 2048) "1")
  (func (export "__alloc") (param $cap i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32) (i32.const 0))
  (func (export "on_tick") (result i32)
    (drop (call $kv_set (i32.const 1024) (i32.const 6) (i32.const 2048) (i32.const 1)))
    (i32.const 0)))"#;

/// 通过 manifest 校验的标准测试 manifest(v2):订阅一个宿主事件与一个插件事件。
pub(crate) const MANIFEST: &str = r#"
plugin_id = "com.example.test"
name = "Test Plugin"
version = "1.0.0"
abi_version = 2
subscribes = ["agent_offline", "plugin_expiry_soon"]
"#;

pub(crate) fn app() -> Arc<App> {
    Arc::new(App::for_test(Db::open(":memory:").unwrap()))
}

pub(crate) fn row(wasm: Vec<u8>) -> PluginRow {
    PluginRow {
        id: 1,
        plugin_id: "com.example.test".into(),
        name: "Test".into(),
        version: "1.0.0".into(),
        manifest_json: MANIFEST.into(),
        wasm_blob: wasm,
        wasm_sha256: String::new(),
        enabled: true,
        status: "enabled".into(),
        last_error: None,
        uploaded_at: 0,
    }
}

pub(crate) fn engine() -> wasmtime::Engine {
    new_engine()
}

pub(crate) fn expiry_event() -> Event {
    Event::Plugin {
        name: "plugin_expiry_soon".into(),
        payload: serde_json::json!({
            "node_id": 7,
            "name": "edge-1",
            "expires_at": "2026-10-01",
            "days_left": 7,
            "threshold_days": 7,
        }),
    }
}

pub(crate) fn compile(wat_text: &str) -> Vec<u8> {
    wat::parse_str(wat_text).unwrap()
}

/// manifest 文本,subscribes 可变。
pub(crate) fn manifest_text(plugin_id: &str, subscribes: &[&str]) -> String {
    let list = subscribes.iter().map(|s| format!("\"{s}\"")).collect::<Vec<_>>().join(", ");
    format!(
        "plugin_id = \"{plugin_id}\"\nname = \"{plugin_id}\"\nversion = \"1.0.0\"\nabi_version = 2\nsubscribes = [{list}]"
    )
}

/// 只写 db(enabled=1),不碰 Registry:给 init 的预加载路径留一个"db 里有、
/// 内存里没有"的起点。
pub(crate) fn insert(app: &App, plugin_id: &str, subscribes: &[&str], wasm: Vec<u8>) -> i64 {
    let row = app
        .db
        .create_plugin(plugin_id, plugin_id, "1.0.0", &manifest_text(plugin_id, subscribes), &wasm, "")
        .unwrap();
    app.db.set_plugin_enabled(row.id, true).unwrap();
    row.id
}

/// db 行 + Registry 加载,返回行 id。
pub(crate) fn install(app: &App, plugin_id: &str, subscribes: &[&str], wasm: Vec<u8>) -> i64 {
    let id = insert(app, plugin_id, subscribes, wasm);
    app.plugins.write().unwrap_or_else(|e| e.into_inner()).enable_plugin(app, id).unwrap();
    id
}

/// App 包进 Arc 后初始化 Registry:真实派发路径需要 Weak 回指 upgrade 成功。
pub(crate) fn runtime_app() -> Arc<App> {
    let app = Arc::new(App::for_test(Db::open(":memory:").unwrap()));
    app.plugins.write().unwrap_or_else(|e| e.into_inner()).init(&app);
    app
}
