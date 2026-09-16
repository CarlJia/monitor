//! finance-stats —— 财务统计插件，monitor-hub wasm 插件 ABI v2 的参考实现。
//!
//! 职责：
//!
//! - 自持全部节点财务数据（价格 / 币种 / 计费周期 / 到期日），存在自己的
//!   `plugin_data` 命名空间里（宿主 node 表不再有这些列）；
//! - 首次运行时经 `nodes_query` 把宿主历史数据导入（一次性、幂等）；
//! - 每小时 tick：用 `http_get` 从 Frankfurter 拉汇率并缓存，再扫描到期日
//!   （到期的在线节点按周期向后滚动；进入阈值窗口的经 `emit_event` 发
//!   `plugin_expiry_soon`）；
//! - `render_page` 渲染统计页（年化续费成本 / 剩余价值 / 到期列表 / 编辑表）；
//! - `on_action` 处理页面交互（切币种 / 保存节点 / 立即刷新汇率）；
//! - `on_cleanup` 清掉已删除节点的残留记录。
//!
//! # 数据模型（plugin_data 的记录）
//!
//! | key | value |
//! |-----|-------|
//! | `config` | `{target_currency, threshold_days, imported}` |
//! | `fx` | `{base, rates: {CUR: number}, fetched_at}` |
//! | `node:<id>` | `{name, price, currency, billing_cycle, expires_at, purchased_at}` |
//!
//! 时间一律向宿主要（`host_now`），日期算术交给 chrono 的 NaiveDate。
//! 时区用 UTC——与面板展示的本地日期可能差一天，这是已知取舍（宿主原 Logic
//! 用本地时区，插件在 wasm 里拿不到时区）。

use chrono::{DateTime, Months, NaiveDate};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::ptr;

// ---------------------------------------------------------------------------
// 宿主函数（ABI v2）：从名为 "host" 的 wasm import 模块导入。
// 签名与错误码见 hub 的 src/plugin/host.rs。
// ---------------------------------------------------------------------------

#[link(wasm_import_module = "host")]
extern "C" {
    /// level：0=debug 1=info 2=warn 3=error。
    #[link_name = "log"]
    fn host_log(level: i32, ptr: i32, len: i32);
    /// 当前 Unix 秒。
    #[link_name = "now"]
    fn host_now() -> i64;
    /// 让宿主经 `__alloc` 分配响应缓冲并记住指针。
    #[link_name = "resp_alloc"]
    fn host_resp_alloc(cap: i32) -> i32;
    /// 只读 https GET。返回写入 resp 缓冲的字节数；负数是错误码。
    #[link_name = "http_get"]
    fn host_http_get(url_ptr: i32, url_len: i32, resp_ptr: i32, resp_cap: i32) -> i32;
    /// 只读节点基础信息：返回写入 out 的字节数（JSON 数组），负数是错误码。
    #[link_name = "nodes_query"]
    fn host_nodes_query(out_ptr: i32, out_cap: i32) -> i32;
    /// 发出一个 `plugin_` 前缀的事件。
    #[link_name = "emit_event"]
    fn host_emit_event(name_ptr: i32, name_len: i32, payload_ptr: i32, payload_len: i32) -> i32;
    /// 写一行插件数据：0 成功，-6 超限，-8 数据库失败。
    #[link_name = "data_put"]
    fn host_data_put(key_ptr: i32, key_len: i32, val_ptr: i32, val_len: i32) -> i32;
    /// 读一行插件数据：写入 out 的字节数，0 无此记录。
    #[link_name = "data_get"]
    fn host_data_get(key_ptr: i32, key_len: i32, out_ptr: i32, out_cap: i32) -> i32;
    /// 删一行插件数据：0 成功。
    #[link_name = "data_delete"]
    fn host_data_delete(key_ptr: i32, key_len: i32) -> i32;
    /// 按前缀列出一段插件数据：写入 out 的字节数（JSON 数组），-8 数据库失败。
    #[link_name = "data_list"]
    fn host_data_list(prefix_ptr: i32, prefix_len: i32, out_ptr: i32, out_cap: i32) -> i32;
}

// ---------------------------------------------------------------------------
// 分配器与内存工具
// ---------------------------------------------------------------------------

static mut HEAP_NEXT: usize = 1024;

/// 宿主写事件/入参、`host_resp_alloc` 回程都走这里。
#[no_mangle]
pub extern "C" fn __alloc(cap: i32) -> i32 {
    if cap <= 0 {
        return 1024;
    }
    unsafe {
        let ptr = HEAP_NEXT;
        HEAP_NEXT += cap as usize + 8;
        ptr as i32
    }
}

fn write_bytes(bytes: &[u8]) -> (i32, i32) {
    let ptr = __alloc(bytes.len().max(1) as i32);
    unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), ptr as *mut u8, bytes.len()) };
    (ptr, bytes.len() as i32)
}

fn write_str(s: &str) -> (i32, i32) {
    write_bytes(s.as_bytes())
}

fn log(level: i32, msg: &str) {
    let (p, l) = write_str(msg);
    unsafe { host_log(level, p, l) };
}

/// 把一段字节当 UTF-8 读回；非法 UTF-8 用有损转换。
fn bytes_to_string(buf: &[u8]) -> String {
    String::from_utf8_lossy(buf).into_owned()
}

// ---------------------------------------------------------------------------
// plugin_data 包装
// ---------------------------------------------------------------------------

/// 缓冲大小：单条节点记录与整段列表都很小（百台机器量级）。
const BUF: usize = 256 * 1024;

fn data_put(key: &str, value: &str) -> bool {
    let (kp, kl) = write_str(key);
    let (vp, vl) = write_str(value);
    unsafe { host_data_put(kp, kl, vp, vl) == 0 }
}

fn data_get(key: &str) -> Option<String> {
    let (kp, kl) = write_str(key);
    let mut buf = vec![0u8; 16 * 1024];
    let n = unsafe { host_data_get(kp, kl, buf.as_mut_ptr() as i32, buf.len() as i32) };
    if n <= 0 {
        return None;
    }
    Some(bytes_to_string(&buf[..n as usize]))
}

fn data_delete(key: &str) {
    let (kp, kl) = write_str(key);
    unsafe { host_data_delete(kp, kl) };
}

/// 列出 `prefix` 前缀的全部记录，返回 (key, data) 列表。
fn data_list(prefix: &str) -> Vec<(String, String)> {
    let (pp, pl) = write_str(prefix);
    let mut buf = vec![0u8; BUF];
    let n = unsafe { host_data_list(pp, pl, buf.as_mut_ptr() as i32, buf.len() as i32) };
    if n <= 0 {
        return Vec::new();
    }
    let text = bytes_to_string(&buf[..n as usize]);
    let rows: Vec<Value> = serde_json::from_str(&text).unwrap_or_default();
    rows.into_iter()
        .filter_map(|r| {
            Some((r.get("key")?.as_str()?.to_owned(), r.get("data")?.as_str()?.to_owned()))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// 领域模型
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeFin {
    name: String,
    price: f64,
    currency: String,
    billing_cycle: String,
    expires_at: Option<String>,
    /// 一次性付费机器的购买日，用于剩余价值折算（周期机器不读）。
    #[serde(default)]
    purchased_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Fx {
    base: String,
    rates: HashMap<String, f64>,
    fetched_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Config {
    #[serde(default = "default_target")]
    target_currency: String,
    #[serde(default = "default_threshold")]
    threshold_days: i64,
    #[serde(default)]
    imported: bool,
}

fn default_target() -> String {
    "CNY".into()
}
fn default_threshold() -> i64 {
    7
}

impl Config {
    fn load() -> Config {
        data_get("config")
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_else(|| Config {
                target_currency: default_target(),
                threshold_days: default_threshold(),
                imported: false,
            })
    }
    fn save(&self) {
        if let Ok(s) = serde_json::to_string(self) {
            data_put("config", &s);
        }
    }
}

/// 计费周期 → 月数。`once` 无周期（不滚动、不进年化）。
fn cycle_months(cycle: &str) -> Option<u32> {
    Some(match cycle {
        "monthly" => 1,
        "quarterly" => 3,
        "semiannual" => 6,
        "yearly" => 12,
        "biennial" => 24,
        "triennial" => 36,
        _ => return None,
    })
}

fn today() -> NaiveDate {
    let secs = unsafe { host_now() };
    DateTime::from_timestamp(secs, 0).map(|d| d.date_naive()).unwrap_or_else(|| NaiveDate::from_ymd_opt(2026, 1, 1).unwrap())
}

/// 节点记录键。
fn node_key(id: i64) -> String {
    format!("node:{id}")
}

fn load_node(id: i64) -> Option<NodeFin> {
    data_get(&node_key(id)).and_then(|s| serde_json::from_str(&s).ok())
}

fn save_node(id: i64, n: &NodeFin) {
    if let Ok(s) = serde_json::to_string(n) {
        data_put(&node_key(id), &s);
    }
}

fn all_nodes() -> Vec<(i64, NodeFin)> {
    data_list("node:")
        .into_iter()
        .filter_map(|(key, data)| {
            let id = key.strip_prefix("node:")?.parse::<i64>().ok()?;
            let n = serde_json::from_str::<NodeFin>(&data).ok()?;
            Some((id, n))
        })
        .collect()
}

fn online_nodes() -> Vec<i64> {
    let mut buf = vec![0u8; BUF];
    let n = unsafe { host_nodes_query(buf.as_mut_ptr() as i32, buf.len() as i32) };
    if n <= 0 {
        return Vec::new();
    }
    let arr: Vec<Value> = serde_json::from_str(&bytes_to_string(&buf[..n as usize])).unwrap_or_default();
    arr.into_iter()
        .filter(|v| v.get("online").and_then(|o| o.as_bool()).unwrap_or(false))
        .filter_map(|v| v.get("id").and_then(|i| i.as_i64()))
        .collect()
}

// ---------------------------------------------------------------------------
// 导入（KTD10：一次性、幂等）
// ---------------------------------------------------------------------------

/// 首次运行时经 nodes_query 读节点、建初始财务记录。宿主 node 表已不再有
/// 财务列，所以导入只建「空财务记录」（price=0、币种默认、无到期日）——
/// 有历史数据的部署在升级前由宿主导出，这里只保证每台机器都有记录可编辑。
/// 幂等：config.imported 为真即跳过。
fn ensure_imported(cfg: &mut Config) {
    if cfg.imported {
        return;
    }
    let known: std::collections::HashSet<i64> = all_nodes().into_iter().map(|(id, _)| id).collect();
    for (id, name) in nodes_basic() {
        if known.contains(&id) {
            continue;
        }
        save_node(
            id,
            &NodeFin {
                name,
                price: 0.0,
                currency: "USD".into(),
                billing_cycle: "monthly".into(),
                expires_at: None,
                purchased_at: Some(today().to_string()),
            },
        );
    }
    cfg.imported = true;
    cfg.save();
}

/// 节点的 (id, name)。供导入建记录与清理对账。
fn nodes_basic() -> Vec<(i64, String)> {
    let mut buf = vec![0u8; BUF];
    let n = unsafe { host_nodes_query(buf.as_mut_ptr() as i32, buf.len() as i32) };
    if n <= 0 {
        return Vec::new();
    }
    let arr: Vec<Value> = serde_json::from_str(&bytes_to_string(&buf[..n as usize])).unwrap_or_default();
    arr.into_iter()
        .filter_map(|v| Some((v.get("id")?.as_i64()?, v.get("name")?.as_str()?.to_owned())))
        .collect()
}

// ---------------------------------------------------------------------------
// 汇率（Frankfurter，KTD2/KTD5）
// ---------------------------------------------------------------------------

/// 拉一次汇率，以目标币种为 base。成功则更新缓存并返回 true。
fn refresh_fx(cfg: &Config) -> bool {
    let base = &cfg.target_currency;
    let url = format!("https://api.frankfurter.dev/v1/latest?base={base}");
    log(1, &format!("finance-stats: 拉取汇率 {url}"));
    let (up, ul) = write_str(&url);
    let mut buf = vec![0u8; 64 * 1024];
    let n = unsafe { host_http_get(up, ul, buf.as_mut_ptr() as i32, buf.len() as i32) };
    if n <= 0 {
        log(2, &format!("finance-stats: 汇率拉取失败（{n}）"));
        return false;
    }
    let body = bytes_to_string(&buf[..n as usize]);
    let v: Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            log(2, &format!("finance-stats: 汇率响应不是 JSON: {e}"));
            return false;
        }
    };
    let rates: HashMap<String, f64> = v
        .get("rates")
        .and_then(|r| serde_json::from_value(r.clone()).ok())
        .unwrap_or_default();
    if rates.is_empty() {
        log(2, "finance-stats: 汇率响应没有 rates");
        return false;
    }
    let fx = Fx { base: base.clone(), rates, fetched_at: unsafe { host_now() } };
    if let Ok(s) = serde_json::to_string(&fx) {
        data_put("fx", &s);
        return true;
    }
    false
}

fn load_fx() -> Option<Fx> {
    data_get("fx").and_then(|s| serde_json::from_str(&s).ok())
}

/// 把一个金额从 `from` 币种换算到目标币种。汇率表以目标币种为 base，
/// `rates[from]` 是「1 目标币 = 多少 from」，故换算为除法。缺汇率返回 None。
fn convert(fx: &Fx, amount: f64, from: &str) -> Option<f64> {
    if from == fx.base {
        return Some(amount);
    }
    fx.rates.get(from).filter(|r| **r != 0.0).map(|r| amount / r)
}

// ---------------------------------------------------------------------------
// 统计
// ---------------------------------------------------------------------------

/// 一台机器的年化成本（原币种）与剩余价值（原币种）。免费的与 once 的按
/// R10 处理。
fn annual_cost(n: &NodeFin) -> Option<f64> {
    let months = cycle_months(&n.billing_cycle)?;
    if n.price <= 0.0 {
        return None; // price=0 不计入汇总
    }
    Some(n.price * (12.0 / months as f64))
}

/// 剩余价值（原币种）。周期机器按周期内未消耗比例；once 按购买日折算；
/// 无到期日视为 0。
fn remaining_value(n: &NodeFin, today: NaiveDate) -> Option<f64> {
    if n.price <= 0.0 {
        return None;
    }
    let expires = n.expires_at.as_deref().and_then(|d| d.parse::<NaiveDate>().ok())?;
    let days_left = (expires - today).num_days();
    if days_left <= 0 {
        return Some(0.0);
    }
    match cycle_months(&n.billing_cycle) {
        Some(months) => {
            // 周期长度按 30 天/月近似（与面板观感一致，不需要公历精确）。
            let cycle_days = (months as f64) * 30.0;
            Some(n.price * (days_left as f64 / cycle_days).min(1.0))
        }
        None => {
            // once：按购买日到到期日的总跨度折算。
            let purchased = n.purchased_at.as_deref().and_then(|d| d.parse::<NaiveDate>().ok());
            match purchased {
                Some(p) => {
                    let total = (expires - p).num_days().max(1) as f64;
                    Some(n.price * (days_left as f64 / total).min(1.0))
                }
                None => Some(0.0),
            }
        }
    }
}

/// 汇总：返回 (年化总成本, 剩余总价值) 在目标币种下的值；任一节点缺汇率
/// 则该项跳过。fx 为 None 时返回 None（汇率不可用）。
fn totals(fx: Option<&Fx>, today: NaiveDate) -> Option<((f64, f64), Vec<String>)> {
    let fx = fx?;
    let mut annual = 0.0;
    let mut remaining = 0.0;
    // 任一节点缺汇率则该项跳过；列出被跳过的币种让面板提示用户。
    let mut missing: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for (_, n) in all_nodes() {
        if let Some(c) = annual_cost(&n) {
            match convert(fx, c, &n.currency) {
                Some(v) => annual += v,
                None => { missing.insert(n.currency.clone()); }
            }
        }
        if let Some(r) = remaining_value(&n, today) {
            match convert(fx, r, &n.currency) {
                Some(v) => remaining += v,
                None => { missing.insert(n.currency.clone()); }
            }
        }
    }
    Some(((annual, remaining), missing.into_iter().collect()))
}

// ---------------------------------------------------------------------------
// 到期扫描（周期性）
// ---------------------------------------------------------------------------

/// 滚动到期日：到期日已过但节点仍在线的，按周期向后滚到未来。返回是否改动。
fn roll_if_online(id: i64, n: &mut NodeFin, online: &[i64], today: NaiveDate) -> bool {
    if !online.contains(&id) {
        return false;
    }
    let Some(months) = cycle_months(&n.billing_cycle) else {
        return false;
    };
    let Some(exp) = n.expires_at.as_deref().and_then(|d| d.parse::<NaiveDate>().ok()) else {
        return false;
    };
    if exp >= today {
        return false;
    }
    let mut next = exp;
    let step = Months::new(months);
    while next < today {
        match next.checked_add_months(step) {
            Some(x) => next = x,
            None => return false,
        }
    }
    n.expires_at = Some(next.to_string());
    true
}

/// 一拍的全部周期工作：导入（幂等）→ 刷新汇率 → 扫描到期。
fn tick() {
    let mut cfg = Config::load();
    ensure_imported(&mut cfg);
    if refresh_fx(&cfg) {
        log(1, "finance-stats: 汇率已更新");
    }
    let today = today();
    let online = online_nodes();
    for (id, mut n) in all_nodes() {
        if roll_if_online(id, &mut n, &online, today) {
            log(1, &format!("finance-stats: 节点 {} 到期日滚动到 {:?}", n.name, n.expires_at));
            save_node(id, &n);
            continue; // 刚滚动的节点 days_left 变大，本轮不再提醒
        }
        let Some(exp) = n.expires_at.as_deref().and_then(|d| d.parse::<NaiveDate>().ok()) else {
            continue;
        };
        let days_left = (exp - today).num_days();
        // 精确等值匹配单一档位，已过期（<=0）不提醒（那是滚动或人工处理的事）。
        if days_left > 0 && days_left == cfg.threshold_days {
            let payload = json!({
                "node_id": id,
                "name": n.name,
                "expires_at": n.expires_at,
                "days_left": days_left,
                "threshold_days": cfg.threshold_days,
            });
            let (np, nl) = write_str("plugin_expiry_soon");
            let (pp, pl) = write_str(&payload.to_string());
            unsafe { host_emit_event(np, nl, pp, pl) };
        }
    }
}

// ---------------------------------------------------------------------------
// 页面渲染（U5）
// ---------------------------------------------------------------------------

/// 支持的展示币种：面板原有的六个 + Frankfurter 覆盖的常见币种。
const CURRENCIES: [&str; 12] =
    ["CNY", "USD", "EUR", "GBP", "JPY", "CAD", "HKD", "AUD", "CHF", "SGD", "KRW", "INR"];

fn build_page() -> Value {
    let mut cfg = Config::load();
    ensure_imported(&mut cfg);
    let today = today();
    let fx = load_fx();
    let target = cfg.target_currency.clone();

    let mut blocks: Vec<Value> = Vec::new();

    // 汇率状态提示。
    match &fx {
        Some(f) => {
            let when = DateTime::from_timestamp(f.fetched_at, 0)
                .map(|d| d.format("%Y-%m-%d %H:%M UTC").to_string())
                .unwrap_or_default();
            blocks.push(json!({
                "type": "notice",
                "text": format!("汇率基准 {}，更新时间 {when}", f.base),
            }));
        }
        None => blocks.push(json!({
            "type": "notice",
            "kind": "warning",
            "text": "汇率不可用——尚未成功拉取过汇率，统计暂缺",
        })),
    }

    // 汇总统计。
    let currency_label = target.clone();
    match totals(fx.as_ref(), today) {
        Some(((annual, remaining), missing)) => {
            blocks.push(json!({
                "type": "stat",
                "items": [
                    {"label": format!("年化续费总成本（{currency_label}）"), "value": format!("{annual:.2}")},
                    {"label": format!("剩余总价值（{currency_label}）"), "value": format!("{remaining:.2}")},
                ],
            }));
            if !missing.is_empty() {
                blocks.push(json!({
                    "type": "notice",
                    "kind": "warning",
                    "text": format!(
                        "部分节点币种未在 Frankfurter 汇率表中（{}），未计入汇总",
                        missing.join("、")
                    ),
                }));
            }
        }
        None => {}
    }

    // 币种切换。
    blocks.push(json!({
        "type": "select",
        "name": "target_currency",
        "label": "展示币种",
        "value": target,
        "options": CURRENCIES,
        "action": "set_currency",
    }));

    // 到期窗口列表。
    let window = cfg.threshold_days;
    let mut due: Vec<Value> = Vec::new();
    let mut all: Vec<Value> = Vec::new();
    for (id, n) in all_nodes() {
        let days_left = n
            .expires_at
            .as_deref()
            .and_then(|d| d.parse::<NaiveDate>().ok())
            .map(|e| (e - today).num_days());
        let free = n.price <= 0.0;
        all.push(json!({
            "id": id,
            "name": n.name,
            "price": n.price,
            "currency": n.currency,
            "billing_cycle": n.billing_cycle,
            "expires_at": n.expires_at,
            "free": free,
        }));
        if let Some(d) = days_left {
            if d >= 0 && d <= window {
                due.push(json!({
                    "name": n.name,
                    "expires_at": n.expires_at,
                    "days_left": d,
                    "free": free,
                }));
            }
        }
    }
    due.sort_by_key(|v| v.get("days_left").and_then(|d| d.as_i64()).unwrap_or(i64::MAX));
    blocks.push(json!({
        "type": "table",
        "title": format!("{window} 天内到期（{}）", due.len()),
        "columns": ["节点", "到期日", "剩余天数", "备注"],
        "rows": due.iter().map(|v| json!([
            v["name"], v["expires_at"], v["days_left"],
            if v["free"].as_bool().unwrap_or(false) { "免费" } else { "" }
        ])).collect::<Vec<_>>(),
    }));

    // 全量编辑表（价格/币种/周期/到期日）。
    blocks.push(json!({
        "type": "form",
        "title": "节点财务数据",
        "action": "save_node",
        "fields": ["name", "price", "currency", "billing_cycle", "expires_at"],
        "rows": all,
    }));

    json!({ "title": "财务统计", "blocks": blocks })
}

// ---------------------------------------------------------------------------
// 页面交互（U5）
// ---------------------------------------------------------------------------

fn handle_action(input: &str) -> Value {
    let req: Value = serde_json::from_str(input).unwrap_or(Value::Null);
    let action = req.get("action").and_then(|a| a.as_str()).unwrap_or("");
    match action {
        "set_currency" => {
            let mut cfg = Config::load();
            if let Some(cur) = req.get("value").and_then(|v| v.as_str()) {
                cfg.target_currency = cur.to_owned();
                cfg.save();
                refresh_fx(&cfg);
            }
            build_page()
        }
        "refresh_fx" => {
            let cfg = Config::load();
            refresh_fx(&cfg);
            build_page()
        }
        "save_node" => {
            if let Some(id) = req.get("id").and_then(|v| v.as_i64()) {
                if let Some(mut n) = load_node(id) {
                    if let Some(v) = req.get("name").and_then(|v| v.as_str()) {
                        n.name = v.to_owned();
                    }
                    if let Some(v) = req.get("price").and_then(|v| v.as_f64()) {
                        n.price = v;
                    }
                    if let Some(v) = req.get("currency").and_then(|v| v.as_str()) {
                        n.currency = v.to_owned();
                    }
                    if let Some(v) = req.get("billing_cycle").and_then(|v| v.as_str()) {
                        n.billing_cycle = v.to_owned();
                    }
                    if let Some(v) = req.get("expires_at").and_then(|v| v.as_str()) {
                        n.expires_at = if v.is_empty() { None } else { Some(v.to_owned()) };
                    }
                    save_node(id, &n);
                }
            }
            build_page()
        }
        _ => build_page(),
    }
}

// ---------------------------------------------------------------------------
// 清理（U9/KTD11）
// ---------------------------------------------------------------------------

/// 删掉已不在宿主节点表里的残留记录，返回清理统计。
fn cleanup() -> Value {
    let live: std::collections::HashSet<i64> = nodes_basic().into_iter().map(|(id, _)| id).collect();
    let mut pruned = 0i64;
    let mut freed = 0i64;
    for (id, _) in all_nodes() {
        if !live.contains(&id) {
            if let Some(data) = data_get(&node_key(id)) {
                freed += data.len() as i64;
            }
            data_delete(&node_key(id));
            pruned += 1;
        }
    }
    json!({ "freed_bytes": freed, "pruned": pruned })
}

// ---------------------------------------------------------------------------
// 导出
// ---------------------------------------------------------------------------

/// 本插件不订阅宿主事件（tick 才是它的工作面），保留导出满足 ABI 契约。
#[no_mangle]
pub extern "C" fn on_event(_ptr: i32, _len: i32) -> i32 {
    0
}

/// 每小时 tick。
#[no_mangle]
pub extern "C" fn on_tick() -> i32 {
    tick();
    0
}

/// 渲染面板页面。
#[no_mangle]
pub extern "C" fn render_page(_ptr: i32, _len: i32) -> i32 {
    respond(&build_page())
}

/// 处理页面交互。
#[no_mangle]
pub extern "C" fn on_action(ptr: i32, len: i32) -> i32 {
    let input = if ptr > 0 && len > 0 {
        bytes_to_string(unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) })
    } else {
        String::new()
    };
    respond(&handle_action(&input))
}

/// 清理残留记录。
#[no_mangle]
pub extern "C" fn on_cleanup(_ptr: i32, _len: i32) -> i32 {
    respond(&cleanup())
}

/// 把一段 JSON 经 host_resp_alloc 的缓冲写回宿主，返回字节数。
fn respond(value: &Value) -> i32 {
    let bytes = serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec());
    let cap = bytes.len() as i32;
    let ptr = unsafe { host_resp_alloc(cap) };
    if ptr <= 0 {
        log(3, "finance-stats: resp_alloc 失败");
        return -1;
    }
    unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), ptr as *mut u8, bytes.len()) };
    cap
}

