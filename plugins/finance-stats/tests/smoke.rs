//! 端到端冒烟测试：先 `cargo build` 出真实的 wasm 产物，再用 wasmtime 复刻
//! hub 的 ABI v2 宿主环境（log / now / resp_alloc / http_get / nodes_query /
//! emit_event / data_put / data_get / data_delete / data_list），驱动插件的
//! on_tick / render_page / on_action / on_cleanup。
//!
//! 插件代码不 mock，只 mock 宿主——与生产路径的差别仅在于 http 不走网络、
//! 数据不落 SQLite。
//!
//! 嵌套 cargo build 用独立的 `--target-dir`（target/smoke）：外层 `cargo test`
//! 持有 target/ 的构建锁，共用一个目录会互相等待。

use std::collections::HashMap;
use std::process::Command;
use std::sync::Mutex;

use chrono::{DateTime, NaiveDate};
use wasmtime::{Caller, Config, Engine, Extern, Linker, Memory, Module, Store};

const FUEL_LIMIT: u64 = 4_000_000;
/// 固定"当前时间"：2027-01-15 前后，测试的日期都相对它构造。
const NOW: i64 = 1_800_000_000;

fn today() -> NaiveDate {
    DateTime::from_timestamp(NOW, 0).unwrap().date_naive()
}

fn build_wasm() -> Vec<u8> {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let target_dir = format!("{manifest_dir}/target/smoke");
    let out = Command::new("cargo")
        .args(["build", "--release", "--target", "wasm32-unknown-unknown", "--target-dir", &target_dir])
        .current_dir(manifest_dir)
        .output()
        .expect("cargo build 应能启动");
    assert!(out.status.success(), "wasm 构建失败：\n{}", String::from_utf8_lossy(&out.stderr));
    std::fs::read(format!("{target_dir}/wasm32-unknown-unknown/release/finance_stats.wasm"))
        .expect("应产出 finance_stats.wasm")
}

fn engine() -> Engine {
    let mut config = Config::new();
    config.consume_fuel(true);
    Engine::new(&config).unwrap()
}

/// 桩宿主的全部状态。
#[derive(Default)]
struct Host {
    /// plugin_data 命名空间（单插件，key → value）。
    data: Mutex<HashMap<String, String>>,
    /// 收到的 emit_event(name, payload)。
    emitted: Mutex<Vec<(String, String)>>,
    /// 节点列表应答（nodes_query）。
    nodes: Mutex<Vec<(i64, String, bool)>>,
    /// http_get 的应答体；None 表示请求失败（返回 -4）。
    http_body: Mutex<Option<String>>,
    /// 最近一次 resp_alloc 拿到的缓冲指针与容量（与生产宿主一致）。
    resp: Mutex<(i32, i32)>,
    logs: Mutex<Vec<(i32, String)>>,
}

// ---- 内存工具 ----

fn read_mem(caller: &mut Caller<'_, Host>, ptr: i32, len: i32) -> Option<Vec<u8>> {
    if ptr <= 0 || len < 0 {
        return None;
    }
    let mem = caller.get_export("memory")?.into_memory()?;
    let data = mem.data(&*caller);
    let start = ptr as usize;
    let end = start.checked_add(len as usize)?;
    Some(data.get(start..end)?.to_vec())
}

fn read_text(caller: &mut Caller<'_, Host>, ptr: i32, len: i32) -> Option<String> {
    String::from_utf8(read_mem(caller, ptr, len)?).ok()
}

fn write_mem(caller: &mut Caller<'_, Host>, ptr: i32, bytes: &[u8]) -> bool {
    if ptr <= 0 {
        return false;
    }
    let Some(mem) = caller.get_export("memory").and_then(Extern::into_memory) else {
        return false;
    };
    let start = ptr as usize;
    let Some(end) = start.checked_add(bytes.len()) else { return false };
    let data = mem.data_mut(&mut *caller);
    let Some(target) = data.get_mut(start..end) else { return false };
    target.copy_from_slice(bytes);
    true
}

fn instantiate(engine: &Engine, wasm: &[u8], host: Host) -> (Store<Host>, wasmtime::Instance) {
    let module = Module::new(engine, wasm).expect("wasm 应能编译");
    let mut store = Store::new(engine, host);
    store.set_fuel(FUEL_LIMIT).unwrap();
    let mut linker: Linker<Host> = Linker::new(engine);

    linker
        .func_wrap("host", "log", |mut caller: Caller<'_, Host>, level: i32, ptr: i32, len: i32| {
            let text = read_text(&mut caller, ptr, len).unwrap_or_default();
            caller.data().logs.lock().unwrap().push((level, text));
        })
        .unwrap();
    linker.func_wrap("host", "now", || -> i64 { NOW }).unwrap();

    linker
        .func_wrap("host", "resp_alloc", |mut caller: Caller<'_, Host>, cap: i32| -> i32 {
            if cap <= 0 {
                return -1;
            }
            let Some(func) = caller.get_export("__alloc").and_then(Extern::into_func) else { return -1 };
            let Ok(typed) = func.typed::<(i32,), i32>(&caller) else { return -1 };
            let ptr = typed.call(&mut caller, (cap,)).unwrap_or(-1);
            if ptr > 0 {
                *caller.data().resp.lock().unwrap() = (ptr, cap);
            }
            ptr
        })
        .unwrap();

    // http_get：把桩应答写回 resp 缓冲；http_body 为 None 时返回 -4（网络失败）。
    linker
        .func_wrap(
            "host",
            "http_get",
            |mut caller: Caller<'_, Host>, url_ptr: i32, url_len: i32, resp_ptr: i32, resp_cap: i32| -> i32 {
                let Some(url) = read_text(&mut caller, url_ptr, url_len) else { return -1 };
                if !url.starts_with("https://") {
                    return -2;
                }
                let body = caller.data().http_body.lock().unwrap().clone();
                let Some(body) = body else { return -4 };
                let n = body.len().min(resp_cap.max(0) as usize);
                if resp_ptr > 0 && n > 0 && !write_mem(&mut caller, resp_ptr, &body.as_bytes()[..n]) {
                    return -1;
                }
                n as i32
            },
        )
        .unwrap();

    // nodes_query：返回 [{id,name,online}]。
    linker
        .func_wrap("host", "nodes_query", |mut caller: Caller<'_, Host>, out_ptr: i32, out_cap: i32| -> i32 {
            let arr: Vec<serde_json::Value> = caller
                .data()
                .nodes
                .lock()
                .unwrap()
                .iter()
                .map(|(id, name, online)| serde_json::json!({"id": id, "name": name, "online": online}))
                .collect();
            let bytes = serde_json::to_vec(&arr).unwrap();
            let n = bytes.len().min(out_cap.max(0) as usize);
            if out_ptr > 0 && n > 0 && !write_mem(&mut caller, out_ptr, &bytes[..n]) {
                return -1;
            }
            n as i32
        })
        .unwrap();

    linker
        .func_wrap(
            "host",
            "emit_event",
            |mut caller: Caller<'_, Host>, np: i32, nl: i32, pp: i32, pl: i32| -> i32 {
                let Some(name) = read_text(&mut caller, np, nl) else { return -1 };
                let Some(payload) = read_text(&mut caller, pp, pl) else { return -1 };
                caller.data().emitted.lock().unwrap().push((name, payload));
                0
            },
        )
        .unwrap();

    linker
        .func_wrap(
            "host",
            "data_put",
            |mut caller: Caller<'_, Host>, kp: i32, kl: i32, vp: i32, vl: i32| -> i32 {
                let Some(key) = read_text(&mut caller, kp, kl) else { return -1 };
                let Some(val) = read_text(&mut caller, vp, vl) else { return -1 };
                caller.data().data.lock().unwrap().insert(key, val);
                0
            },
        )
        .unwrap();

    linker
        .func_wrap(
            "host",
            "data_get",
            |mut caller: Caller<'_, Host>, kp: i32, kl: i32, out_ptr: i32, out_cap: i32| -> i32 {
                let Some(key) = read_text(&mut caller, kp, kl) else { return -1 };
                let value = caller.data().data.lock().unwrap().get(&key).cloned();
                let Some(value) = value else { return 0 };
                let n = value.len().min(out_cap.max(0) as usize);
                if out_ptr > 0 && n > 0 && !write_mem(&mut caller, out_ptr, &value.as_bytes()[..n]) {
                    return -1;
                }
                n as i32
            },
        )
        .unwrap();

    linker
        .func_wrap("host", "data_delete", |mut caller: Caller<'_, Host>, kp: i32, kl: i32| -> i32 {
            let Some(key) = read_text(&mut caller, kp, kl) else { return -1 };
            caller.data().data.lock().unwrap().remove(&key);
            0
        })
        .unwrap();

    linker
        .func_wrap(
            "host",
            "data_list",
            |mut caller: Caller<'_, Host>, pp: i32, pl: i32, out_ptr: i32, out_cap: i32| -> i32 {
                let Some(prefix) = read_text(&mut caller, pp, pl) else { return -1 };
                let mut rows: Vec<serde_json::Value> = caller
                    .data()
                    .data
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(k, _)| k.starts_with(&prefix))
                    .map(|(k, v)| serde_json::json!({"key": k, "data": v}))
                    .collect();
                rows.sort_by(|a, b| a["key"].as_str().cmp(&b["key"].as_str()));
                let bytes = serde_json::to_vec(&rows).unwrap();
                let n = bytes.len().min(out_cap.max(0) as usize);
                if out_ptr > 0 && n > 0 && !write_mem(&mut caller, out_ptr, &bytes[..n]) {
                    return -1;
                }
                n as i32
            },
        )
        .unwrap();

    let instance = linker.instantiate(&mut store, &module).expect("应满足导出契约并实例化");
    (store, instance)
}

/// 调一个无参导出（on_tick）。
fn call_unit(store: &mut Store<Host>, instance: &wasmtime::Instance, name: &str) -> i32 {
    let f = instance.get_typed_func::<(), i32>(&mut *store, name).unwrap();
    f.call(&mut *store, ()).unwrap()
}

/// 调一个 (ptr,len)->i32 导出（render_page / on_action / on_cleanup），把输入
/// 经 __alloc 写进内存，返回写回 resp 缓冲的 JSON 文本。
fn call_json(store: &mut Store<Host>, instance: &wasmtime::Instance, name: &str, input: &str) -> String {
    let alloc = instance.get_typed_func::<(i32,), i32>(&mut *store, "__alloc").unwrap();
    let f = instance.get_typed_func::<(i32, i32), i32>(&mut *store, name).unwrap();
    let mem: Memory = instance.get_memory(&mut *store, "memory").unwrap();
    let bytes = input.as_bytes();
    let ptr = alloc.call(&mut *store, (bytes.len().max(1) as i32,)).unwrap();
    mem.data_mut(&mut *store)[ptr as usize..ptr as usize + bytes.len()].copy_from_slice(bytes);
    let n = f.call(&mut *store, (ptr, bytes.len() as i32)).unwrap();
    assert!(n >= 0, "{name} 返回错误码 {n}");
    // 响应写在插件最近一次 resp_alloc 记下的缓冲里（与生产宿主同约定）。
    let (resp_ptr, resp_cap) = *store.data().resp.lock().unwrap();
    assert!(resp_ptr > 0, "{name} 未调用 resp_alloc");
    let len = (n as usize).min(resp_cap.max(0) as usize);
    let data = mem.data(&*store);
    String::from_utf8_lossy(&data[resp_ptr as usize..resp_ptr as usize + len]).into_owned()
}

/// 拉一次 tick，并返回 emitted 的新增数量。
fn tick(store: &mut Store<Host>, instance: &wasmtime::Instance) -> Vec<String> {
    let before = store.data().emitted.lock().unwrap().len();
    call_unit(store, instance, "on_tick");
    store.data().emitted.lock().unwrap()[before..].iter().map(|(n, _)| n.clone()).collect()
}

fn fx_json(base: &str) -> String {
    format!(
        r#"{{"amount":1.0,"base":"{base}","date":"2027-01-15","rates":{{"USD":0.14,"CNY":1.0,"EUR":0.13}}}}"#
    )
}

/// 造一个 seed 插件数据记录的辅助：直接往桩宿主的数据表里塞。
fn seed(store: &Store<Host>, key: &str, value: &str) {
    store.data().data.lock().unwrap().insert(key.to_owned(), value.to_owned());
}

fn node_record(name: &str, price: f64, currency: &str, cycle: &str, expires: Option<&str>) -> String {
    serde_json::json!({
        "name": name, "price": price, "currency": currency,
        "billing_cycle": cycle, "expires_at": expires, "purchased_at": null,
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

/// 首次 tick 导入所有节点记录；再次 tick 不重复导入（幂等）。
#[test]
fn tick_imports_nodes_once() {
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default();
    host.nodes.lock().unwrap().extend([(1, "edge-1".into(), true), (2, "edge-2".into(), false)]);
    host.http_body.lock().unwrap().replace(fx_json("CNY"));
    let (mut store, instance) = instantiate(&engine, &wasm, host);

    tick(&mut store, &instance);
    let data = store.data().data.lock().unwrap().clone();
    assert!(data.contains_key("node:1"), "导入应建 node:1");
    assert!(data.contains_key("node:2"));
    assert!(data.contains_key("fx"), "汇率缓存已写");

    // 手工改一条，再 tick 不应被覆盖（imported 标志生效）。
    seed(&store, "node:1", &node_record("edge-1", 42.0, "USD", "monthly", None));
    tick(&mut store, &instance);
    assert!(store.data().data.lock().unwrap()["node:1"].contains("42"), "二次 tick 不重导入");
}

/// Covers AE4：到期已过但在线的节点，日期向后滚动、不发事件。
#[test]
fn overdue_online_node_rolls_forward() {
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default();
    host.nodes.lock().unwrap().push((1, "edge-1".into(), true));
    host.http_body.lock().unwrap().replace(fx_json("CNY"));
    let (mut store, instance) = instantiate(&engine, &wasm, host);

    let past = (today() - chrono::Duration::days(1)).to_string();
    seed(&store, "node:1", &node_record("edge-1", 10.0, "USD", "monthly", Some(&past)));
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);

    let emitted = tick(&mut store, &instance);
    assert!(emitted.is_empty(), "滚动的节点不发提醒");
    let rec = store.data().data.lock().unwrap()["node:1"].clone();
    assert!(!rec.contains(&past), "到期日应已向后滚动");
}

/// Covers AE5：进入阈值窗口的节点 emit plugin_expiry_soon，载荷字段完整。
#[test]
fn threshold_node_emits_event() {
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default();
    host.nodes.lock().unwrap().push((7, "edge-7".into(), false));
    host.http_body.lock().unwrap().replace(fx_json("CNY"));
    let (mut store, instance) = instantiate(&engine, &wasm, host);

    let soon = (today() + chrono::Duration::days(7)).to_string();
    seed(&store, "node:7", &node_record("edge-7", 10.0, "USD", "monthly", Some(&soon)));
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);

    tick(&mut store, &instance);
    let events = store.data().emitted.lock().unwrap().clone();
    assert_eq!(events.len(), 1, "进入阈值窗口发一次");
    assert_eq!(events[0].0, "plugin_expiry_soon");
    let payload: serde_json::Value = serde_json::from_str(&events[0].1).unwrap();
    assert_eq!(payload["node_id"], 7);
    assert_eq!(payload["days_left"], 7);
    assert_eq!(payload["threshold_days"], 7);
    assert_eq!(payload["expires_at"], soon);
}

/// Covers AE3：尚无汇率缓存时页面显示「汇率不可用」。
#[test]
fn page_warns_when_fx_unavailable() {
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default();
    // http_body 保持 None：拉取失败。 // 拉取失败
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);

    call_unit(&mut store, &instance, "on_tick"); // 尝试拉，失败
    let page = call_json(&mut store, &instance, "render_page", "{}");
    assert!(page.contains("汇率不可用"), "页面应提示汇率不可用：{page}");
}

/// Covers AE1 + AE6：多币种年化汇总换算正确；price=0 不计入。
#[test]
fn page_totals_convert_and_skip_free() {
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default();
    // rates（base=CNY）：USD=0.14 → 1 USD = 1/0.14 CNY。
    host.http_body.lock().unwrap().replace(fx_json("CNY"));
    let (mut store, instance) = instantiate(&engine, &wasm, host);

    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);
    seed(&store, "node:1", &node_record("paid", 10.0, "USD", "monthly", None));
    seed(&store, "node:2", &node_record("free", 0.0, "USD", "monthly", None));

    call_unit(&mut store, &instance, "on_tick");
    let page: serde_json::Value =
        serde_json::from_str(&call_json(&mut store, &instance, "render_page", "{}")).unwrap();
    let stats = page["blocks"].as_array().unwrap().iter().find(|b| b["type"] == "stat").unwrap();
    let annual = stats["items"][0]["value"].as_str().unwrap().parse::<f64>().unwrap();
    // 10 USD * 12 = 120 USD/年 → /0.14 ≈ 857.14 CNY。免费节点不计入。
    assert!((annual - 120.0 / 0.14).abs() < 1.0, "年化应约 {:.2}，实际 {annual}", 120.0 / 0.14);
}

/// Covers AE2：周期内剩余比例折算。
#[test]
fn remaining_value_prorates_within_cycle() {
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default();
    host.http_body.lock().unwrap().replace(fx_json("CNY"));
    let (mut store, instance) = instantiate(&engine, &wasm, host);

    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);
    // 月付 10 USD，30 天周期还剩 15 天 → 5 USD → /0.14 CNY。
    let in15 = (today() + chrono::Duration::days(15)).to_string();
    seed(&store, "node:1", &node_record("edge", 10.0, "USD", "monthly", Some(&in15)));

    call_unit(&mut store, &instance, "on_tick");
    let page: serde_json::Value =
        serde_json::from_str(&call_json(&mut store, &instance, "render_page", "{}")).unwrap();
    let stats = page["blocks"].as_array().unwrap().iter().find(|b| b["type"] == "stat").unwrap();
    let remaining = stats["items"][1]["value"].as_str().unwrap().parse::<f64>().unwrap();
    let expect = 5.0 / 0.14;
    assert!((remaining - expect).abs() < 1.0, "剩余价值应约 {expect:.2}，实际 {remaining}");
}

/// set_currency 持久化到 config，并立即按新币种重算。
#[test]
fn set_currency_persists_and_recomputes() {
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default();
    host.http_body.lock().unwrap().replace(fx_json("CNY"));
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);

    call_json(&mut store, &instance, "on_action", r#"{"action":"set_currency","value":"EUR"}"#);
    let cfg = store.data().data.lock().unwrap()["config"].clone();
    assert!(cfg.contains("EUR"), "config 应持久化新币种：{cfg}");
}

/// 清理：删掉已不在节点表里的残留记录，返回统计。
#[test]
fn cleanup_prunes_deleted_nodes() {
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default();
    host.nodes.lock().unwrap().push((1, "kept".into(), true)); // 只保留 1
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "node:1", &node_record("kept", 1.0, "USD", "monthly", None));
    seed(&store, "node:99", &node_record("gone", 1.0, "USD", "monthly", None));

    let out: serde_json::Value =
        serde_json::from_str(&call_json(&mut store, &instance, "on_cleanup", "{}")).unwrap();
    assert_eq!(out["pruned"], 1, "只清掉已删除的节点");
    let data = store.data().data.lock().unwrap().clone();
    assert!(data.contains_key("node:1"), "保留的节点不动");
    assert!(!data.contains_key("node:99"), "已删除节点的记录被清");
}
