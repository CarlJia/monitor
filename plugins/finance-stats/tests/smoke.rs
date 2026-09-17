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

/// 构建产物在同一进程内只编译一次：并行跑的各条测试都调 `build_wasm`，不缓存
/// 就会全部堵在 `target/smoke` 的构建锁上，一遍遍地等同一个结果。
static WASM: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();

fn build_wasm() -> Vec<u8> {
    WASM.get_or_init(build_wasm_uncached).clone()
}

fn build_wasm_uncached() -> Vec<u8> {
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
    /// 覆盖 http_get 的返回码：Some 时直接返回该码，不看 http_body。
    /// 用来驱动分类表里那些桩宿主本来产不出的码（-5 非 2xx、未登记码等）。
    http_error: Mutex<Option<i32>>,
    /// http_get 的调用次数——用来断言"该拉的拉了、不该拉的一次都没拉"。
    http_calls: Mutex<u32>,
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
                *caller.data().http_calls.lock().unwrap() += 1;
                if !url.starts_with("https://") {
                    return -2;
                }
                // 指定的错误码优先于应答体：-5（非 2xx）这类码要看宿主的状态
                // 分支，桩宿主用一个响应体表达不了。
                let overridden = *caller.data().http_error.lock().unwrap();
                if let Some(code) = overridden {
                    return code;
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

/// 与插件 `fmt_ts` 同格式的时间文案。
fn ts(secs: i64) -> String {
    DateTime::from_timestamp(secs, 0).unwrap().format("%Y-%m-%d %H:%M UTC").to_string()
}

/// 直接塞进 plugin_data 的汇率缓存记录（`fx_json` 是 http 应答体，字段不同）。
fn fx_record(base: &str, fetched_at: i64) -> String {
    serde_json::json!({
        "base": base,
        "rates": {"USD": 0.14, "CNY": 1.0, "EUR": 0.13},
        "fetched_at": fetched_at,
    })
    .to_string()
}

/// 直接塞进 plugin_data 的拉取失败记录。
fn fx_status_record(reason: &str, attempted_at: i64) -> String {
    serde_json::json!({ "reason": reason, "attempted_at": attempted_at }).to_string()
}

/// http_get 的调用次数。
fn http_calls(store: &Store<Host>) -> u32 {
    *store.data().http_calls.lock().unwrap()
}

/// 页面里全部 notice 块的文本。
fn notice_texts(page: &serde_json::Value) -> Vec<String> {
    page["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|b| b["type"] == "notice")
        .map(|b| b["text"].as_str().unwrap_or_default().to_owned())
        .collect()
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

/// 页面里的 form 块。
fn form_block(page: &serde_json::Value) -> &serde_json::Value {
    page["blocks"].as_array().unwrap().iter().find(|b| b["type"] == "form").expect("页面应有 form 块")
}

/// 按字段名取一条字段声明。
fn field_of<'a>(form: &'a serde_json::Value, name: &str) -> &'a serde_json::Value {
    form["fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["name"] == name)
        .unwrap_or_else(|| panic!("表单应有字段 {name}：{form}"))
}

/// (kind, text)，响应没带 toast 时 panic。
fn toast_of(resp: &serde_json::Value) -> (String, String) {
    let toast = resp.get("toast").unwrap_or_else(|| panic!("响应应带 toast：{resp}"));
    (
        toast["kind"].as_str().unwrap_or_default().to_owned(),
        toast["text"].as_str().unwrap_or_else(|| panic!("toast 应带文案：{toast}")).to_owned(),
    )
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
/// U3 之后 render_page 自己也会试一次（KTD3），失败原因与尝试时间一并上提示条。
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
    assert!(page.contains("尚未成功拉取过汇率"), "页面应说明从未成功拉取过：{page}");
}

/// Covers AE3（R4/KTD3）：无缓存 + 拉取成功——打开页面即触发一次 http_get，
/// 页面立刻有汇率更新时间与统计块，不必等 tick。
#[test]
fn render_page_fetches_fx_when_cache_missing() {
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default();
    host.http_body.lock().unwrap().replace(fx_json("CNY"));
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);
    seed(&store, "node:1", &node_record("paid", 10.0, "USD", "monthly", None));

    let page: serde_json::Value =
        serde_json::from_str(&call_json(&mut store, &instance, "render_page", "{}")).unwrap();

    assert_eq!(http_calls(&store), 1, "首屏应恰好拉一次汇率");
    let notices = notice_texts(&page);
    assert!(
        notices.iter().any(|t| t.contains(&format!("汇率基准 CNY，更新时间 {}", ts(NOW)))),
        "页面应显示刷新后的汇率时间：{notices:?}"
    );
    assert!(
        page["blocks"].as_array().unwrap().iter().any(|b| b["type"] == "stat"),
        "拉到汇率后应有统计块：{page}"
    );
    let data = store.data().data.lock().unwrap().clone();
    assert!(data.contains_key("fx"), "首屏拉取的汇率应落缓存");
    assert!(!data.contains_key("fx_status"), "成功的拉取不留失败记录");
}

/// Covers AE4（R5/KTD4）：无缓存 + 拉取失败——warning 条含「尚未成功拉取过汇率」
/// 与具体失败原因（http 负错误码分类）和尝试时间。
#[test]
fn render_page_warns_with_failure_reason_when_fetch_fails() {
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default(); // http_body 保持 None → 桩宿主返回 -4
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);

    let page: serde_json::Value =
        serde_json::from_str(&call_json(&mut store, &instance, "render_page", "{}")).unwrap();

    assert_eq!(http_calls(&store), 1, "首屏应尝试过一次");
    let text = &notice_texts(&page)[0];
    assert!(text.contains("尚未成功拉取过汇率"), "应区分「从未成功拉取过」：{text}");
    assert!(text.contains("网络请求失败或超时"), "应带上可读的失败原因：{text}");
    assert!(text.contains(&ts(NOW)), "应带上尝试时间：{text}");
    let data = store.data().data.lock().unwrap().clone();
    assert!(!data.contains_key("fx"), "失败的拉取不写缓存");
    assert!(data.contains_key("fx_status"), "失败原因应落盘：{data:?}");
}

/// KTD3：有缓存（且陈旧）时打开页面不拉——刷新交给每小时 tick。
#[test]
fn render_page_does_not_fetch_when_cache_present() {
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default(); // 拉的话会失败：计数为 0 才说明根本没试
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);
    seed(&store, "fx", &fx_record("CNY", NOW - 86_400));

    let page: serde_json::Value =
        serde_json::from_str(&call_json(&mut store, &instance, "render_page", "{}")).unwrap();

    assert_eq!(http_calls(&store), 0, "有缓存就不该再拉");
    assert_eq!(
        notice_texts(&page),
        vec![format!("汇率基准 CNY，更新时间 {}", ts(NOW - 86_400))],
        "有缓存且无失败记录时提示条维持现状"
    );
}

/// R5/KTD4：有缓存 + 存在失败记录——保留汇率基准条，并追加一行失败原因。
#[test]
fn cached_page_appends_last_refresh_failure() {
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default();
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);
    seed(&store, "fx", &fx_record("CNY", NOW - 7_200));
    seed(&store, "fx_status", &fx_status_record("网络请求失败或超时", NOW));

    let page: serde_json::Value =
        serde_json::from_str(&call_json(&mut store, &instance, "render_page", "{}")).unwrap();

    assert_eq!(http_calls(&store), 0, "有缓存不拉");
    let notices = notice_texts(&page);
    assert!(
        notices.iter().any(|t| t.contains(&format!("汇率基准 CNY，更新时间 {}", ts(NOW - 7_200)))),
        "汇率基准条保留：{notices:?}"
    );
    assert!(
        notices
            .iter()
            .any(|t| t.contains(&format!("最近一次刷新 {} 失败：网络请求失败或超时", ts(NOW)))),
        "应追加失败原因与尝试时间：{notices:?}"
    );
}

/// KTD3：保存这条路不拉汇率——保存不阻塞在网络往返上（切币种与手动刷新
/// 各自显式拉一次，见 set_currency / refresh_fx_toasts_success_and_failure）。
#[test]
fn save_node_action_does_not_fetch_fx() {
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default(); // 拉了会失败，正好用计数断言"没拉"
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);
    seed(&store, "node:1", &node_record("edge-1", 10.0, "USD", "monthly", None));

    let resp: serde_json::Value = serde_json::from_str(&call_json(
        &mut store,
        &instance,
        "on_action",
        r#"{"action":"save_node","id":1,"name":"edge-renamed","price":10.0,
            "currency":"USD","billing_cycle":"monthly","expires_at":""}"#,
    ))
    .unwrap();

    assert_eq!(http_calls(&store), 0, "保存这条路径不该拉汇率");
    assert_eq!(toast_of(&resp), ("success".into(), "已保存".into()));
    // 没缓存时页面照样给出「尚未成功拉取过」的 warning（现有语义不变）。
    let notices = notice_texts(&resp);
    assert!(
        notices.iter().any(|t| t.contains("尚未成功拉取过汇率")),
        "无缓存时 action 返回的页面仍应 warning：{notices:?}"
    );
}

/// R5/KTD4：拉取成功后失败记录被清除。
#[test]
fn successful_refresh_clears_failure_status() {
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default();
    host.http_body.lock().unwrap().replace(fx_json("CNY"));
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);
    seed(&store, "fx_status", &fx_status_record("网络请求失败或超时", NOW - 3_600));

    let resp: serde_json::Value = serde_json::from_str(&call_json(
        &mut store,
        &instance,
        "on_action",
        r#"{"action":"refresh_fx"}"#,
    ))
    .unwrap();

    assert_eq!(toast_of(&resp).0, "success");
    let data = store.data().data.lock().unwrap().clone();
    assert!(data.contains_key("fx"), "成功应写入汇率缓存");
    assert!(!data.contains_key("fx_status"), "成功应清掉失败记录：{data:?}");
    assert!(
        !notice_texts(&resp).iter().any(|t| t.contains("失败")),
        "清掉失败记录后页面不应再提失败：{:?}",
        notice_texts(&resp)
    );
}

/// R5/KTD4：失败记录写在 refresh_fx 内部，tick 这条路径同样留痕；
/// 原因按错误种类分类（非 JSON 响应、响应缺 rates）。
#[test]
fn failure_reasons_are_classified_and_recorded_everywhere() {
    let engine = engine();
    let wasm = build_wasm();

    // 网络失败（http 负错误码 -4）：tick 也要记。
    let host = Host::default();
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);
    tick(&mut store, &instance);
    let st: serde_json::Value =
        serde_json::from_str(&store.data().data.lock().unwrap()["fx_status"]).unwrap();
    assert_eq!(st["reason"], "网络请求失败或超时", "负错误码应分类成可读文案：{st}");
    assert_eq!(st["attempted_at"], NOW, "应记下尝试时间");

    // 响应不是 JSON。
    let host = Host::default();
    host.http_body.lock().unwrap().replace("<html>502</html>".into());
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);
    tick(&mut store, &instance);
    let st: serde_json::Value =
        serde_json::from_str(&store.data().data.lock().unwrap()["fx_status"]).unwrap();
    let reason = st["reason"].as_str().unwrap();
    assert!(reason.contains("JSON"), "解析失败要说明是 JSON 问题：{reason}");

    // 是 JSON 但没有 rates。
    let host = Host::default();
    host.http_body.lock().unwrap().replace(r#"{"amount":1.0,"base":"CNY"}"#.into());
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);
    tick(&mut store, &instance);
    let st: serde_json::Value =
        serde_json::from_str(&store.data().data.lock().unwrap()["fx_status"]).unwrap();
    assert!(
        st["reason"].as_str().unwrap().contains("rates"),
        "缺 rates 要有自己的文案：{st}"
    );

    // 非 2xx（宿主错误码 -5）：生产里由 host_funcs 的响应状态分支产生，
    // 桩宿主靠 http_error 覆盖出来。
    let host = Host::default();
    *host.http_error.lock().unwrap() = Some(-5);
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);
    tick(&mut store, &instance);
    let st: serde_json::Value =
        serde_json::from_str(&store.data().data.lock().unwrap()["fx_status"]).unwrap();
    assert!(
        st["reason"].as_str().unwrap().contains("非 2xx"),
        "-5 应描述成非 2xx 响应：{st}"
    );

    // 未登记的错误码（-7 是 emit_event 的，不该出现在 http 分类里）：兜底
    // 文案要原样带上码值，操作员才有线索去查。
    let host = Host::default();
    *host.http_error.lock().unwrap() = Some(-7);
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);
    tick(&mut store, &instance);
    let st: serde_json::Value =
        serde_json::from_str(&store.data().data.lock().unwrap()["fx_status"]).unwrap();
    assert!(
        st["reason"].as_str().unwrap().contains("宿主返回错误码 -7"),
        "未登记的码要走兜底并带上码值：{st}"
    );
}

/// 页面钩子的开销随机器数线性增长,而宿主给的预算是每次调用固定的(见仓库
/// README「资源限制」的钩子那一档)。按那个预算跑一个真实规模的页面:一百台
/// 机器的财务记录必须渲染得出来——超出预算的那次,生产里就是点页面回 502
/// (实测 20 台约 150 万 fuel、100 台约 580 万)。
#[test]
fn page_renders_within_the_production_fuel_budget() {
    /// 宿主的 `plugin.hook_fuel_limit` 默认值,见仓库 README 的资源限制表。
    const PROD_HOOK_FUEL: u64 = 20_000_000;
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default();
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    store.set_fuel(PROD_HOOK_FUEL).unwrap();

    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);
    for i in 1..=100 {
        seed(
            &store,
            &format!("node:{i}"),
            &node_record(&format!("edge-{i}"), 10.0, "USD", "monthly", Some("2027-01-20")),
        );
    }

    // call_json 在 trap 时 panic:渲染不出来就是这条断言先炸。
    let page: serde_json::Value =
        serde_json::from_str(&call_json(&mut store, &instance, "render_page", "{}")).unwrap();
    let rows = page["blocks"].as_array().unwrap().iter().find(|b| b["type"] == "form").unwrap()["rows"]
        .as_array()
        .unwrap()
        .len();
    assert_eq!(rows, 100, "页面要列出全部一百台机器");
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

/// set_currency 持久化到 config，并立即按新币种重算；成功返回提示。
#[test]
fn set_currency_persists_and_recomputes() {
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default();
    host.http_body.lock().unwrap().replace(fx_json("CNY"));
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);

    let resp: serde_json::Value = serde_json::from_str(&call_json(
        &mut store,
        &instance,
        "on_action",
        r#"{"action":"set_currency","value":"EUR"}"#,
    ))
    .unwrap();
    let cfg = store.data().data.lock().unwrap()["config"].clone();
    assert!(cfg.contains("EUR"), "config 应持久化新币种：{cfg}");
    assert_eq!(toast_of(&resp), ("success".into(), "已切换币种".into()), "切币种成功应提示");
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

/// Covers AE1（R1/R2）：form 字段是新式对象声明，列头为中文，币种与周期是
/// 带选项的下拉；选项集合与插件的 `CURRENCIES` / `cycle_months` 覆盖集一致。
#[test]
fn form_fields_declare_labels_and_select_options() {
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default();
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);

    let page: serde_json::Value =
        serde_json::from_str(&call_json(&mut store, &instance, "render_page", "{}")).unwrap();
    let form = form_block(&page);

    // 字段顺序即列顺序；每个字段都是对象形态（KTD1 的新式声明）。
    let names: Vec<&str> =
        form["fields"].as_array().unwrap().iter().map(|f| f["name"].as_str().unwrap()).collect();
    assert_eq!(
        names,
        ["name", "price", "currency", "billing_cycle", "expires_at"],
        "字段集合与顺序不变：{form}"
    );

    // 中文列头（R1）。
    for (name, label) in [
        ("name", "节点名"),
        ("price", "价格"),
        ("currency", "币种"),
        ("billing_cycle", "计费周期"),
        ("expires_at", "到期日"),
    ] {
        assert_eq!(field_of(form, name)["label"], label, "{name} 应有中文标签：{form}");
    }

    // 控件类型（R2）。
    assert_eq!(field_of(form, "price")["type"], "number");
    assert_eq!(field_of(form, "expires_at")["type"], "date");
    assert_eq!(field_of(form, "name")["type"], "text");
    assert_eq!(field_of(form, "currency")["type"], "select");
    assert_eq!(field_of(form, "billing_cycle")["type"], "select");

    // 币种下拉选项 = CURRENCIES。
    let currencies: Vec<&str> = field_of(form, "currency")["options"]
        .as_array()
        .expect("币种字段应带 options")
        .iter()
        .map(|o| o.as_str().unwrap())
        .collect();
    assert_eq!(
        currencies,
        ["CNY", "USD", "EUR", "GBP", "JPY", "CAD", "HKD", "AUD", "CHF", "SGD", "KRW", "INR"],
        "币种选项应与 CURRENCIES 一致"
    );

    // 周期下拉：覆盖 cycle_months 认得的全部周期，并含 once。
    let cycles: Vec<&str> = field_of(form, "billing_cycle")["options"]
        .as_array()
        .expect("周期字段应带 options")
        .iter()
        .map(|o| o.as_str().unwrap())
        .collect();
    for c in ["monthly", "quarterly", "semiannual", "yearly", "biennial", "triennial"] {
        assert!(cycles.contains(&c), "周期选项应覆盖 cycle_months 的 {c}：{cycles:?}");
    }
    assert!(cycles.contains(&"once"), "周期选项应含 once：{cycles:?}");
}

/// Covers AE2（R3）：save_node 成功返回「已保存」提示，节点记录落盘，统计重算。
#[test]
fn save_node_toasts_and_persists() {
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default();
    // rates（base=CNY）：EUR=0.13 → 1 EUR = 1/0.13 CNY。
    host.http_body.lock().unwrap().replace(fx_json("CNY"));
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);
    seed(&store, "node:1", &node_record("edge-1", 10.0, "USD", "monthly", None));
    call_unit(&mut store, &instance, "on_tick"); // 先把汇率缓存灌进去，页面才有统计块

    let resp: serde_json::Value = serde_json::from_str(&call_json(
        &mut store,
        &instance,
        "on_action",
        r#"{"action":"save_node","id":1,"name":"edge-renamed","price":42.5,
            "currency":"EUR","billing_cycle":"yearly","expires_at":"2027-06-01"}"#,
    ))
    .unwrap();

    assert_eq!(toast_of(&resp), ("success".into(), "已保存".into()), "保存成功应提示");

    let rec: serde_json::Value =
        serde_json::from_str(&store.data().data.lock().unwrap()["node:1"]).unwrap();
    assert_eq!(rec["name"], "edge-renamed");
    assert_eq!(rec["price"], 42.5);
    assert_eq!(rec["currency"], "EUR");
    assert_eq!(rec["billing_cycle"], "yearly");
    assert_eq!(rec["expires_at"], "2027-06-01");

    // 响应里的新页面描述已按新数据重算：42.5 EUR/年 → /0.13 CNY。
    let stats = resp["blocks"].as_array().unwrap().iter().find(|b| b["type"] == "stat").unwrap();
    let annual = stats["items"][0]["value"].as_str().unwrap().parse::<f64>().unwrap();
    assert!((annual - 42.5 / 0.13).abs() < 1.0, "年化应随保存重算，实际 {annual}");
}

/// save_node 失败路径：目标记录不存在时不谎报成功，也不凭空建记录。
#[test]
fn save_node_without_record_toasts_error() {
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default();
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);

    let resp: serde_json::Value = serde_json::from_str(&call_json(
        &mut store,
        &instance,
        "on_action",
        r#"{"action":"save_node","id":404,"price":9.0}"#,
    ))
    .unwrap();
    let (kind, text) = toast_of(&resp);
    assert_eq!(kind, "error", "保存失败应给 error 提示");
    assert!(text.contains("不存在"), "文案应说明未保存：{text}");
    assert!(
        !store.data().data.lock().unwrap().contains_key("node:404"),
        "失败的保存不应建出记录"
    );
}

/// refresh_fx 的两个分支各有对应文案：拉到汇率提示成功，拉不到提示失败。
#[test]
fn refresh_fx_toasts_success_and_failure() {
    let engine = engine();
    let wasm = build_wasm();

    // 成功：http 有应答，缓存写入。
    let host = Host::default();
    host.http_body.lock().unwrap().replace(fx_json("CNY"));
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);
    let ok: serde_json::Value = serde_json::from_str(&call_json(
        &mut store,
        &instance,
        "on_action",
        r#"{"action":"refresh_fx"}"#,
    ))
    .unwrap();
    assert_eq!(toast_of(&ok).0, "success", "刷新成功应给 success 提示");
    assert!(store.data().data.lock().unwrap().contains_key("fx"), "成功应写入汇率缓存");

    // 失败：http 返回错误码（桩宿主 -4），缓存不写、提示为失败。
    let host = Host::default(); // http_body 保持 None
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);
    let bad: serde_json::Value = serde_json::from_str(&call_json(
        &mut store,
        &instance,
        "on_action",
        r#"{"action":"refresh_fx"}"#,
    ))
    .unwrap();
    let (kind, text) = toast_of(&bad);
    assert_eq!(kind, "error", "刷新失败应给 error 提示");
    assert!(text.contains("汇率刷新失败"), "文案应说明失败：{text}");
    // 没缓存：文案要与页面的「尚未成功拉取过汇率，统计暂缺」一致，不能说沿用缓存。
    assert!(text.contains("尚未成功拉取过汇率"), "无缓存时文案应说明统计暂缺：{text}");
    assert!(!text.contains("沿用"), "无缓存时不该谎称沿用旧缓存：{text}");
    // 失败原因取自插件刚落盘的记录——不硬编码原因文本，但仍能挡住「把原因丢掉」。
    let persisted: serde_json::Value =
        serde_json::from_str(&store.data().data.lock().unwrap()["fx_status"]).unwrap();
    let reason = persisted["reason"].as_str().unwrap();
    assert!(text.contains(reason), "文案应带上落盘的失败原因 {reason:?}：{text}");
    assert!(
        !store.data().data.lock().unwrap().contains_key("fx"),
        "失败的刷新不应写入汇率缓存"
    );
}

/// R5/KTD4：refresh_fx 失败但**有**旧缓存时，toast 说明统计沿用缓存并给出
/// 原因，而不是像无缓存那样说统计暂缺。
#[test]
fn refresh_fx_failure_toast_mentions_cache_and_reason() {
    let engine = engine();
    let wasm = build_wasm();
    let host = Host::default(); // http_body 保持 None → 拉取失败
    let (mut store, instance) = instantiate(&engine, &wasm, host);
    seed(&store, "config", r#"{"target_currency":"CNY","threshold_days":7,"imported":true}"#);
    seed(&store, "fx", &fx_record("CNY", NOW - 86_400));

    let resp: serde_json::Value = serde_json::from_str(&call_json(
        &mut store,
        &instance,
        "on_action",
        r#"{"action":"refresh_fx"}"#,
    ))
    .unwrap();

    let (kind, text) = toast_of(&resp);
    assert_eq!(kind, "error", "刷新失败应给 error 提示");
    assert!(text.contains("沿用最近一次缓存"), "有缓存时文案应说明沿用缓存：{text}");
    assert!(!text.contains("统计暂缺"), "有缓存时不该说统计暂缺：{text}");
    let persisted: serde_json::Value =
        serde_json::from_str(&store.data().data.lock().unwrap()["fx_status"]).unwrap();
    let reason = persisted["reason"].as_str().unwrap();
    assert!(text.contains(reason), "文案应带上落盘的失败原因 {reason:?}：{text}");
    // 旧缓存仍在，统计照旧有依据。
    assert_eq!(store.data().data.lock().unwrap()["fx"], fx_record("CNY", NOW - 86_400));
}
