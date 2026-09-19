//! 13 个宿主函数的**唯一实现**:插件经名为 `host` 的 import 模块看到的全部面。
//!
//! 一份闭包、两个宿主:`linker::<PluginState>`(monitor 主 crate,委托
//! `Arc<App>`)与 `linker::<ContractState>`(契约替身,内存后端)。签名/返回码/
//! 限额改一边,另一边编译不过——这是"契约与生产不漂移"的机制本身。
//!
//! 每个函数的签名、返回码与限额见其注释(错误码统一为负数,表见
//! [`crate::error_codes`])。
//!
//! **闭包只做参数与内存的搬运**:它对宿主世界的一切了解都经 [`Host`] /
//! [`HasScratch`]。宿主侧的失败原因(kv 读库失败、emit 失败等)由实现在自己那
//! 一侧打日志——那里才拿得到错误链。

use anyhow::Result;
use chrono::Utc;
use tracing::warn;
use wasmtime::{Caller, Extern, Linker};

use crate::constants::{HTTP_RESP_MAX, KV_VALUE_MAX, PLUGIN_DATA_MAX, PLUGIN_EVENT_PREFIX, RECORD_MAX};
use crate::error_codes::{ERR_BOUNDS, ERR_DB, ERR_QUOTA};
use crate::host::{HasScratch, Host, HttpMethod};
use crate::kv::setting_key;

// ---------------------------------------------------------------------------
// 线性内存读写与写回计划
// ---------------------------------------------------------------------------

/// 从插件线性内存读 `[ptr, ptr+len)`。返回 `None` 表示越界。宿主函数绝不能
/// panic(会把整个进程带走),所以一切访问都从这里走、先检查后拷贝。
fn read_mem<S>(caller: &mut Caller<'_, S>, ptr: i32, len: i32) -> Option<Vec<u8>> {
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
fn read_text<S>(caller: &mut Caller<'_, S>, ptr: i32, len: i32) -> Option<String> {
    let bytes = read_mem(caller, ptr, len)?;
    String::from_utf8(bytes).ok()
}

/// 往插件线性内存写字节。越界返回 false。
fn write_mem<S>(caller: &mut Caller<'_, S>, ptr: i32, bytes: &[u8]) -> bool {
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

/// http 响应的写回计划(纯函数,便于单测):决定写到哪个缓冲、最多写多少字节。
///
/// - 选缓冲:显式 `resp_ptr > 0` 优先,否则回落最近一次 `host_resp_alloc`
///   (传入 `last`);都不可用返回 `None`——请求已发出,宿主返回 0 字节而不是
///   报错。
/// - 有效容量 = `min(声明 cap, max)`:既是有界下载的上限(读到即停,超出的
///   字节丢弃——与"整读后截断"同效,但不会把大响应体整个拉进内存),也是最终
///   写回的截断长度。`bytes_len` 传 `usize::MAX` 时返回的第二项就是纯容量,
///   下载前据此定界。
///
/// 容量按 `max` 封顶——http 响应用 [`HTTP_RESP_MAX`],`data_list`/`nodes_query`
/// 这类可能远大于单个响应的结果另有更大的上限(见 [`PLUGIN_DATA_MAX`])。
fn resp_write_plan_capped(
    resp_ptr: i32,
    resp_cap: i32,
    last: (i32, i32),
    bytes_len: usize,
    max: usize,
) -> Option<(i32, usize)> {
    let (ptr, cap) = if resp_ptr > 0 { (resp_ptr, resp_cap) } else { last };
    if ptr <= 0 || cap < 0 {
        return None;
    }
    let cap = (cap as usize).min(max);
    Some((ptr, bytes_len.min(cap)))
}

/// http 响应体与其余小结果的写回计划,上限 [`HTTP_RESP_MAX`]。
fn resp_write_plan(resp_ptr: i32, resp_cap: i32, last: (i32, i32), bytes_len: usize) -> Option<(i32, usize)> {
    resp_write_plan_capped(resp_ptr, resp_cap, last, bytes_len, HTTP_RESP_MAX)
}

/// 不超过 `max` 的最大字符边界偏移:截断必须落在边界上,否则切出的字节不是
/// 合法 UTF-8。detail 的带省略号截断与 host_kv_get 的前缀截断共用(一个加
/// 省略号一个不加,共用的是边界计算)。
pub fn char_boundary_end(s: &str, max: usize) -> usize {
    let mut end = s.len().min(max);
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    end
}

// ---------------------------------------------------------------------------
// 13 个宿主函数
// ---------------------------------------------------------------------------

/// 注册 13 个宿主函数。每次实例化都重建:Linker 不能跨 Store 复用已定义的
/// Func,重建的开销微秒级,正确性优先。
pub fn linker<S: Host + HasScratch + 'static>(engine: &wasmtime::Engine) -> Result<Linker<S>> {
    let mut linker: Linker<S> = Linker::new(engine);

    // host_log(level, ptr, len):0=debug 1=info 2=warn 3=error。越界或非法 UTF-8
    // 时记一条宿主侧 warn、内容按空处理——日志不能让插件把进程带崩。
    linker.func_wrap("host", "log", |mut caller: Caller<'_, S>, level: i32, ptr: i32, len: i32| {
        let plugin_id = caller.data().plugin_id().to_owned();
        let text = match read_text(&mut caller, ptr, len) {
            Some(text) => text,
            None => {
                // 宿主自己发现的坏参数:只进 hub 日志,不进 detail——detail 是
                // 插件自己的话,混进宿主的声音会让人分不清谁在说。
                warn!(plugin = %plugin_id, "host_log 越界或非法 UTF-8: ptr={ptr} len={len}");
                String::new()
            }
        };
        // 空文本不记:越界分支上面已经落了日志,再记一条空行是噪声。
        if !text.is_empty() {
            caller.data().push_log(&text);
        }
        match level {
            0 => tracing::debug!(plugin = %plugin_id, "{text}"),
            1 => tracing::info!(plugin = %plugin_id, "{text}"),
            2 => warn!(plugin = %plugin_id, "{text}"),
            _ => {
                tracing::error!(plugin = %plugin_id, "{text}");
            }
        }
    })?;

    // host_now() -> i64:Unix 秒。
    linker.func_wrap("host", "now", || -> i64 { Utc::now().timestamp() })?;

    // host_kv_get(key_ptr, key_len, out_ptr, out_cap) -> i32:
    //   >0 写入 out 的字节数;0 无值;-1 越界/非法 UTF-8;-8 读库失败。值超 out_cap
    //   时在 UTF-8 字符边界截断——插件拿到的是前缀,长度对得上。
    linker.func_wrap(
        "host",
        "kv_get",
        |mut caller: Caller<'_, S>, key_ptr: i32, key_len: i32, out_ptr: i32, out_cap: i32| -> i32 {
            // 墙钟预算检查(#3):kv 词表没有专门的预算错误码,复用 -1(负数让
            // 插件能感知预算耗尽并退出循环;正常路径不会与越界混淆——越界是
            // 参数问题,预算是时间问题,都会让插件放弃本次调用)。
            if std::time::Instant::now() >= caller.data().deadline() {
                warn!(plugin = %caller.data().plugin_id(), "kv_get 超出派发墙钟预算,拒绝");
                return ERR_BOUNDS;
            }
            let Some(key) = read_text(&mut caller, key_ptr, key_len) else {
                return ERR_BOUNDS;
            };
            let plugin_id = caller.data().plugin_id().to_owned();
            // 读库失败给 -8(数据库错误)而不是 0:0 的含义是「无值或空」,把库故障
            // 也归进去,插件就分不出「没配」与「库坏了」。旧插件把任何 <=0 都当无值,
            // 所以行为不变,只是现在**能**区分。
            let value = match caller.data().kv_get(&setting_key(&plugin_id, &key)) {
                Ok(value) => value,
                Err(()) => return ERR_DB,
            };
            let Some(value) = value else {
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
        |mut caller: Caller<'_, S>, key_ptr: i32, key_len: i32, val_ptr: i32, val_len: i32| -> i32 {
            let Some(key) = read_text(&mut caller, key_ptr, key_len) else {
                return ERR_BOUNDS;
            };
            let Some(value) = read_text(&mut caller, val_ptr, val_len) else {
                return ERR_BOUNDS;
            };
            if value.len() > KV_VALUE_MAX {
                warn!(plugin = %caller.data().plugin_id(), key = %key, "host_kv_set 值超过 {} 字节上限", KV_VALUE_MAX);
                return ERR_BOUNDS;
            }
            let plugin_id = caller.data().plugin_id().to_owned();
            if caller.data().kv_set(&setting_key(&plugin_id, &key), &value).is_err() {
                return -2;
            }
            0
        },
    )?;

    // host_resp_alloc(cap) -> i32:宿主无法直接在 wasm 堆上分配,采用生态标准的
    // allocator 回环——回调模块自己导出的 `__alloc(cap) -> ptr`,把指针记入
    // scratch 并返回给插件。模块未导出 `__alloc`(或分配失败/返回非正指针)
    // 时返回 -1;调用方因此要导出它,模块契约在加载时已检查。
    linker.func_wrap("host", "resp_alloc", |mut caller: Caller<'_, S>, cap: i32| -> i32 {
        if cap <= 0 {
            return ERR_BOUNDS;
        }
        let Some(func) = caller.get_export("__alloc").and_then(Extern::into_func) else {
            warn!(plugin = %caller.data().plugin_id(), "host_resp_alloc 找不到 __alloc 导出");
            return ERR_BOUNDS;
        };
        let Ok(typed) = func.typed::<(i32,), i32>(&caller) else {
            return ERR_BOUNDS;
        };
        match typed.call(&mut caller, (cap,)) {
            Ok(ptr) if ptr > 0 => {
                caller.data_mut().set_resp(ptr, cap);
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
        |mut caller: Caller<'_, S>,
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
            let plugin_id = caller.data().plugin_id().to_owned();
            // 墙钟预算检查(#3):fuel 不计量宿主侧执行,超时也只 detach 任务;
            // 入口拒绝让 wasm 侧的调用循环每轮拿到 -4,配合 fuel 兜底终止循环。
            // -4 沿用"网络失败"码,语义是"本次请求不发出:预算耗尽"。
            if std::time::Instant::now() >= caller.data().deadline() {
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
            let last = caller.data().resp();
            // 下载上限同时约束读取(读到即停)与写回截断;没有可用缓冲也按硬上限
            // 有界下载——读完丢弃,不能不设界。
            let download_cap = resp_write_plan(resp_ptr, resp_cap, last, usize::MAX)
                .map(|(_, cap)| cap)
                .unwrap_or(HTTP_RESP_MAX);
            // 请求的构造、SSRF 预检与有界下载都在实现的 http_request 里(真宿主是
            // plugin_http_fetch),这里只做内存写回。
            let bytes = match caller.data().http_request(
                HttpMethod::Post,
                &url,
                Some(body),
                download_cap,
                caller.data().deadline(),
            ) {
                Ok(bytes) => bytes,
                Err(code) => return code,
            };
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
        |mut caller: Caller<'_, S>, url_ptr: i32, url_len: i32, resp_ptr: i32, resp_cap: i32| -> i32 {
            let Some(url) = read_text(&mut caller, url_ptr, url_len) else {
                return ERR_BOUNDS;
            };
            let plugin_id = caller.data().plugin_id().to_owned();
            if std::time::Instant::now() >= caller.data().deadline() {
                warn!(plugin = %plugin_id, "host_http_get 超出派发墙钟预算,拒绝");
                return -4;
            }
            if !url.starts_with("https://") {
                warn!(plugin = %plugin_id, "host_http_get 拒绝非 https URL");
                return -2;
            }
            let last = caller.data().resp();
            // 下载上限同时约束读取(读到即停)与写回截断;没有可用缓冲也按硬上限
            // 有界下载——读完丢弃,不能不设界。
            let download_cap = resp_write_plan(resp_ptr, resp_cap, last, usize::MAX)
                .map(|(_, cap)| cap)
                .unwrap_or(HTTP_RESP_MAX);
            let bytes = match caller.data().http_request(
                HttpMethod::Get,
                &url,
                None,
                download_cap,
                caller.data().deadline(),
            ) {
                Ok(bytes) => bytes,
                Err(code) => return code,
            };
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

    // host_nodes_query(out_ptr, out_cap) -> i32:
    //   只读节点基础信息(R1/KTD2):返回 JSON 数组
    //   `[{"id":1,"name":"edge-1","online":true},...]`,写回 out,返回字节数。
    //   只回 id/name/online:财务字段(price/currency/...)自 v2 起归财务插件的
    //   plugin_data,这个函数读不到它们,也不该读——插件首次启用时只按 id 建
    //   空白记录,旧值需在插件页面重录。
    linker.func_wrap(
        "host",
        "nodes_query",
        |mut caller: Caller<'_, S>, out_ptr: i32, out_cap: i32| -> i32 {
            let plugin_id = caller.data().plugin_id().to_owned();
            if std::time::Instant::now() >= caller.data().deadline() {
                warn!(plugin = %plugin_id, "host_nodes_query 超出派发墙钟预算,拒绝");
                return -4;
            }
            let nodes = match caller.data().nodes_query() {
                Ok(nodes) => nodes,
                Err(()) => return ERR_DB,
            };
            let arr: Vec<serde_json::Value> = nodes
                .iter()
                .map(|n| {
                    serde_json::json!({
                        "id": n.id,
                        "name": n.name,
                        "online": n.online,
                    })
                })
                .collect();
            let bytes = serde_json::to_vec(&arr).unwrap_or_else(|_| b"[]".to_vec());
            // 节点表可能几百台,远超 http 响应的 64 KiB 上限。按 plugin_data
            // 配额量级给上限:仍放不下就明确报错,**不截断**——截断的 JSON 在
            // 插件侧会退化成"空列表",而空列表在这里意味着清空自己的数据。
            let (ptr, n) = match resp_write_plan_capped(
                out_ptr,
                out_cap,
                caller.data().resp(),
                bytes.len(),
                PLUGIN_DATA_MAX as usize,
            ) {
                Some(plan) => plan,
                None => return 0,
            };
            if n < bytes.len() {
                warn!(plugin = %caller.data().plugin_id(), "nodes_query 结果 {} 字节超出插件缓冲上限", bytes.len());
                return ERR_QUOTA;
            }
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
        |mut caller: Caller<'_, S>,
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
            let plugin_id = caller.data().plugin_id().to_owned();
            if !name.starts_with(PLUGIN_EVENT_PREFIX) || name.len() <= PLUGIN_EVENT_PREFIX.len() {
                warn!(plugin = %plugin_id, name = %name, "emit_event 事件名必须以 plugin_ 开头");
                return -7;
            }
            let Ok(payload) = serde_json::from_str::<serde_json::Value>(&payload_text) else {
                warn!(plugin = %plugin_id, "emit_event payload 不是合法 JSON");
                return ERR_BOUNDS;
            };
            match caller.data().emit_event(&name, payload) {
                Ok(()) => 0,
                Err(()) => ERR_DB,
            }
        },
    )?;

    // host_data_put(key_ptr, key_len, val_ptr, val_len) -> i32:
    //   0 成功(新建或覆盖);-1 越界/非法 UTF-8;-6 超限;-8 数据库失败。
    //   单记录上限 RECORD_MAX、单插件总配额 PLUGIN_DATA_MAX(替换同 key 时
    //   只计增量,KTD3)。
    linker.func_wrap(
        "host",
        "data_put",
        |mut caller: Caller<'_, S>, key_ptr: i32, key_len: i32, val_ptr: i32, val_len: i32| -> i32 {
            let Some(key) = read_text(&mut caller, key_ptr, key_len) else {
                return ERR_BOUNDS;
            };
            let Some(value) = read_text(&mut caller, val_ptr, val_len) else {
                return ERR_BOUNDS;
            };
            if key.is_empty() || value.len() > RECORD_MAX {
                warn!(plugin = %caller.data().plugin_id(), "data_put 记录超限或 key 为空");
                return ERR_QUOTA;
            }
            let plugin_id = caller.data().plugin_id().to_owned();
            // 配额检查与写入在实现里一步做完:分成"读用量→判→写"三步时,同一
            // 插件的两次并发写会各自通过检查,合起来越过上限(KTD3)。
            match caller.data().data_put(&plugin_id, &key, &value) {
                Ok(true) => 0,
                Ok(false) => {
                    warn!(plugin = %plugin_id, "data_put 超出单插件配额");
                    ERR_QUOTA
                }
                Err(()) => ERR_DB,
            }
        },
    )?;

    // host_data_get(key_ptr, key_len, out_ptr, out_cap) -> i32:
    //   >=0 写入 out 的字节数;0 无此记录;-1 越界/非法 UTF-8。
    linker.func_wrap(
        "host",
        "data_get",
        |mut caller: Caller<'_, S>, key_ptr: i32, key_len: i32, out_ptr: i32, out_cap: i32| -> i32 {
            let Some(key) = read_text(&mut caller, key_ptr, key_len) else {
                return ERR_BOUNDS;
            };
            let plugin_id = caller.data().plugin_id().to_owned();
            let Some(value) = caller.data().data_get(&plugin_id, &key).unwrap_or(None) else {
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
        |mut caller: Caller<'_, S>, key_ptr: i32, key_len: i32| -> i32 {
            let Some(key) = read_text(&mut caller, key_ptr, key_len) else {
                return ERR_BOUNDS;
            };
            let plugin_id = caller.data().plugin_id().to_owned();
            match caller.data().data_delete(&plugin_id, &key) {
                Ok(()) => 0,
                Err(()) => ERR_DB,
            }
        },
    )?;

    // host_data_list(prefix_ptr, prefix_len, out_ptr, out_cap) -> i32:
    //   >=0 写入 out 的字节数;前缀过滤;-1 越界/非法 UTF-8;-8 数据库失败。
    //   返回 `[{"key":"node:1","data":"..."},...]`。
    linker.func_wrap(
        "host",
        "data_list",
        |mut caller: Caller<'_, S>,
         prefix_ptr: i32,
         prefix_len: i32,
         out_ptr: i32,
         out_cap: i32|
         -> i32 {
            let Some(prefix) = read_text(&mut caller, prefix_ptr, prefix_len) else {
                return ERR_BOUNDS;
            };
            let plugin_id = caller.data().plugin_id().to_owned();
            let rows = match caller.data().data_list(&plugin_id, &prefix) {
                Ok(rows) => rows,
                Err(()) => return ERR_DB,
            };
            let arr: Vec<serde_json::Value> =
                rows.into_iter().map(|(key, data)| serde_json::json!({ "key": key, "data": data })).collect();
            let bytes = serde_json::to_vec(&arr).unwrap_or_else(|_| b"[]".to_vec());
            // 同 nodes_query:按 plugin_data 配额量级给上限,放不下就报错而不是
            // 截断(截断后的 JSON 会被插件 `unwrap_or_default()` 成空表)。
            let Some((ptr, n)) = resp_write_plan_capped(
                out_ptr,
                out_cap,
                caller.data().resp(),
                bytes.len(),
                PLUGIN_DATA_MAX as usize,
            ) else {
                return 0;
            };
            if n < bytes.len() {
                warn!(plugin = %caller.data().plugin_id(), "data_list 结果 {} 字节超出插件缓冲上限", bytes.len());
                return ERR_QUOTA;
            }
            if !write_mem(&mut caller, ptr, &bytes[..n]) {
                return ERR_BOUNDS;
            }
            n as i32
        },
    )?;

    Ok(linker)
}

// ---------------------------------------------------------------------------
// tests:契约 crate 自己这一层的纯函数
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn char_boundary_end_never_splits_a_character() {
        assert_eq!(char_boundary_end("abc", 2), 2);
        // "中文" 每字 3 字节:上限 4 只能切到 3。
        assert_eq!(char_boundary_end("中文字", 4), 3);
        assert_eq!(char_boundary_end("中文字", 9), 9);
        assert_eq!(char_boundary_end("", 4), 0);
    }
}
