//! tg-notify —— Telegram 通知示例插件，monitor-hub wasm 插件 ABI v2 的
//! 参考实现。ABI 的权威定义见仓库 `src/plugin/` 顶部的模块文档。
//!
//! 一个插件要做的全部事情：
//!
//! 1. 从 `"host"` 模块导入宿主函数（log / now / kv_get / kv_set /
//!    resp_alloc / http_post；v2 另有 http_get / nodes_query /
//!    emit_event / data_put / data_get / data_delete / data_list）；
//! 2. 导出 `memory`、`__alloc`、`on_event`（声明 tick/page/cleanup 时另有
//!    对应导出——本插件都不声明）；
//! 3. 在 `on_event(ptr, len)` 里解析事件 JSON，读自己的 kv 配置，发一次
//!    https POST，返回 0 表示成功。
//!
//! 本插件订阅宿主的离线/在线事件与财务插件发出的 `plugin_expiry_soon`，
//! 把通知渲染成中文文案发到 Telegram 的 `sendMessage`。渠道配置（bot_token
//! / chat_id）由面板的 kv 编辑器写入，运行时经 `host_kv_get` 读取——插件的
//! kv 命名空间是 `plugin.<plugin_id>:<key>`，`<plugin_id>` 取自 plugin.toml，
//! key 里只要写 `bot_token` / `chat_id`。
//!
//! # on_event 的返回码
//!
//! | 码 | 含义 |
//! |----|------|
//! | 0  | 成功（sendMessage 返回 2xx） |
//! | 1  | 事件 JSON 解析失败 |
//! | 2  | kv 里没有 `bot_token` |
//! | 3  | kv 里没有 `chat_id` |
//! | 11–15 | http 失败：10 减去宿主错误码（11=参数越界，12=非 https，13=非 POST，14=网络失败，15=非 2xx） |

use std::ptr;

use serde::Deserialize;
use serde_json::json;

// ---------------------------------------------------------------------------
// 宿主函数（R8）：全部从名为 "host" 的 wasm import 模块导入。
// 签名与错误码见 src/plugin.rs 的 host_linker。
// ---------------------------------------------------------------------------

#[link(wasm_import_module = "host")]
extern "C" {
    /// level：0=debug 1=info 2=warn 3=error。
    #[link_name = "log"]
    fn host_log(level: i32, ptr: i32, len: i32);
    /// 当前 Unix 秒。wasm 里没有时钟，时间一律向宿主要。
    #[link_name = "now"]
    fn host_now() -> i64;
    /// 发一个 `Content-Type: application/json` 的 https POST。返回写入 resp
    /// 缓冲的字节数；负数是错误码（-1 越界 -2 非 https -3 非 POST -4 网络
    /// -5 非 2xx）。
    #[link_name = "http_post"]
    fn host_http_post(
        method_ptr: i32,
        method_len: i32,
        url_ptr: i32,
        url_len: i32,
        body_ptr: i32,
        body_len: i32,
        resp_ptr: i32,
        resp_cap: i32,
    ) -> i32;
    /// 读 kv：返回写入 out 的字节数，0 = 无值，-1 = 越界/非法 UTF-8。
    #[link_name = "kv_get"]
    fn host_kv_get(key_ptr: i32, key_len: i32, out_ptr: i32, out_cap: i32) -> i32;
    /// 写 kv（值上限 8 KiB）：0 成功，-1 越界/超限，-2 写库失败。本插件只读
    /// 配置不写状态，声明保留是为了展示完整的宿主函数面。
    #[allow(dead_code)]
    #[link_name = "kv_set"]
    fn host_kv_set(key_ptr: i32, key_len: i32, val_ptr: i32, val_len: i32) -> i32;
    /// 让宿主经 `__alloc` 分配响应缓冲并记住指针。本插件自己 `__alloc` 后把
    /// 指针直接传给 http_post，效果相同，因此声明但未使用——保留声明是为了
    /// 展示完整的宿主函数面。
    #[allow(dead_code)]
    #[link_name = "resp_alloc"]
    fn host_resp_alloc(cap: i32) -> i32;
}

// ---------------------------------------------------------------------------
// 分配器与内存工具
// ---------------------------------------------------------------------------

/// 线性内存里的堆顶。起始 1024，避开 wasm 栈（栈从 0 向下长）。每次派发宿主
/// 都新建实例，bump 分配永不归还也不会泄漏到下一次调用。
static mut HEAP_NEXT: usize = 1024;

/// 宿主写事件载荷、`host_resp_alloc` 回程都走这里：分配 cap 字节，返回指针。
#[no_mangle]
pub extern "C" fn __alloc(cap: i32) -> i32 {
    if cap <= 0 {
        return 0;
    }
    unsafe {
        let p = HEAP_NEXT;
        // 4 字节对齐，避免未对齐访问的额外指令开销。
        let size = (cap as usize + 3) & !3;
        HEAP_NEXT += size;
        p as i32
    }
}

/// 把字节写进线性内存（经 `__alloc`），返回 (ptr, len)。分配失败返回 (0, 0)。
fn write_bytes(bytes: &[u8]) -> (i32, i32) {
    // 空串也分配 1 字节，保证返回非 0 指针——宿主对空载荷也认。
    let ptr = __alloc(bytes.len().max(1) as i32);
    if ptr <= 0 {
        return (0, 0);
    }
    unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), ptr as *mut u8, bytes.len()) };
    (ptr, bytes.len() as i32)
}

/// `write_bytes` 的字符串版本。
fn write_str(s: &str) -> (i32, i32) {
    write_bytes(s.as_bytes())
}

/// 从线性内存切出事件载荷。ptr/len 由宿主传入，负值或 0 指针视为坏载荷。
fn read_payload(ptr: i32, len: i32) -> Option<&'static [u8]> {
    if ptr <= 0 || len <= 0 {
        return None;
    }
    Some(unsafe { std::slice::from_raw_parts(ptr as *const u8, len as usize) })
}

// ---------------------------------------------------------------------------
// 事件载荷
// ---------------------------------------------------------------------------

/// v2 的事件词表，与 hub 的 `notification_bus::Event` 序列化形状一致（按字段名
/// 反序列化，新增字段被自动忽略，旧插件不会因为词表扩展而坏掉）。v2 起宿主
/// 自身的到期检测退役，到期通知改由财务插件经 `emit_event` 发出，事件名带
/// `plugin_` 前缀——这里用 `rename` 把它映到带标签的变体上。
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(dead_code)]
enum Event {
    /// 财务插件发出的到期提醒（原宿主的 expiry_soon，字段未变）。
    #[serde(rename = "plugin_expiry_soon")]
    PluginExpirySoon {
        node_id: i64,
        name: String,
        expires_at: String,
        days_left: i64,
        threshold_days: i64,
    },
    AgentOffline {
        node_id: i64,
        name: String,
        observed_at: i64,
        last_seen_at: i64,
    },
    AgentOnline {
        node_id: i64,
        name: String,
        observed_at: i64,
    },
}

/// 按事件类型渲染中文通知文案。
fn render(event: &Event) -> String {
    match event {
        Event::PluginExpirySoon { name, expires_at, days_left, .. } => {
            format!("⏰ 节点 {name} 将于 {expires_at} 到期（剩 {days_left} 天）")
        }
        Event::AgentOffline { name, last_seen_at, .. } => {
            // wasm 里没有时钟：当前时间向宿主要，静默时长算给人看。
            let now = unsafe { host_now() };
            let silent_for = (now - last_seen_at).max(0);
            format!("🔴 节点 {name} 已离线（最后上报于 {silent_for} 秒前）")
        }
        Event::AgentOnline { name, .. } => format!("🟢 节点 {name} 已恢复在线"),
    }
}

// ---------------------------------------------------------------------------
// kv 配置
// ---------------------------------------------------------------------------

/// 读一个 kv 配置项。无值（返回 0）、越界（负数）或非法 UTF-8 都归成 None。
fn kv_get_string(key: &str) -> Option<String> {
    let (kptr, klen) = write_str(key);
    if kptr <= 0 {
        return None;
    }
    // bot token 约 46 字节、chat_id 更短；512 字节绰绰有余。
    let mut buf = [0u8; 512];
    let n = unsafe { host_kv_get(kptr, klen, buf.as_mut_ptr() as i32, buf.len() as i32) };
    if n <= 0 {
        return None;
    }
    Some(String::from_utf8_lossy(&buf[..n as usize]).into_owned())
}

// ---------------------------------------------------------------------------
// 事件入口
// ---------------------------------------------------------------------------

/// 处理一个事件。返回码语义见模块文档的表格。
#[no_mangle]
pub extern "C" fn on_event(ptr: i32, len: i32) -> i32 {
    // 1) 解析事件。
    let payload = match read_payload(ptr, len) {
        Some(bytes) => bytes,
        None => return 1,
    };
    let event: Event = match serde_json::from_slice(payload) {
        Ok(event) => event,
        Err(_) => {
            log(3, "tg-notify: 事件载荷不是合法的事件 JSON");
            return 1;
        }
    };

    // 2) 读渠道配置。没配置是操作员可修复的状态：返回明确的错误码而不是
    //    静默成功——静默成功会让"通知没发出去"变成一个谜。
    let Some(token) = kv_get_string("bot_token") else {
        log(2, "tg-notify: kv 里没有 bot_token；请在面板的插件 KV 编辑器里填写");
        return 2;
    };
    let Some(chat_id) = kv_get_string("chat_id") else {
        log(2, "tg-notify: kv 里没有 chat_id；请在面板的插件 KV 编辑器里填写");
        return 3;
    };

    // 3) 构造 sendMessage 请求。token 内嵌在 URL 里，所以日志只打 host。
    let text = render(&event);
    let url = format!("https://api.telegram.org/bot{token}/sendMessage");
    let body = json!({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
    });
    let body = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());

    let (method_ptr, method_len) = write_str("POST");
    let (url_ptr, url_len) = write_str(&url);
    let (body_ptr, body_len) = write_bytes(&body);
    if method_ptr <= 0 || url_ptr <= 0 || body_ptr <= 0 {
        log(3, "tg-notify: 分配请求缓冲失败");
        return 11;
    }
    // 4 KiB 的响应缓冲：sendMessage 的响应是一小段 JSON，4 KiB 足够看清 ok 字段。
    const RESP_CAP: i32 = 4096;
    let resp_ptr = __alloc(RESP_CAP);

    // 4) 发送。返回负数是宿主错误码：10 - code 落在 11..=19，与其它错误码错开。
    let n = unsafe {
        host_http_post(method_ptr, method_len, url_ptr, url_len, body_ptr, body_len, resp_ptr, RESP_CAP)
    };
    if n < 0 {
        log(3, &format!("tg-notify: sendMessage 失败，宿主错误码 {n}（-1 越界 -2 非 https -3 非 POST -4 网络 -5 非 2xx -9 私有地址被拒）"));
        return 10 - n;
    }
    0
}

/// 经 `host_log` 打一条日志（level：0=debug 1=info 2=warn 3=error）。
fn log(level: i32, msg: &str) {
    let (ptr, len) = write_str(msg);
    if ptr > 0 {
        unsafe { host_log(level, ptr, len) };
    }
}
