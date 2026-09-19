//! 宿主函数返回的错误码。与限额一样,单点在契约 crate:monitor 主 crate 的
//! `plugin::host` 反过来 `pub use` 这里,两处不会各抄一份。
//!
//! 成功:kv_get / http_post / http_get 返回写入字节数,其余返回 0。
//!
//! | 码 | 含义 |
//! |----|------|
//! | -1 | 内存越界 / 非法 UTF-8 / 值超限 / `__alloc` 缺失或失败 |
//! | -2 | http:URL 不是 `https://`(或 kv:数据库写入失败) |
//! | -3 | http:method 不是 `POST` |
//! | -4 | http:网络请求失败 / 宿主不在异步运行时上下文 / 墙钟预算耗尽 |
//! | -5 | http:响应状态非 2xx |
//! | -6 | data:记录或单插件配额超限(v2) |
//! | -7 | emit_event:事件名不以 `plugin_` 开头(v2) |
//! | -8 | 数据库错误:kv_get / nodes_query / emit_event / data 系列(v2) |
//! | -9 | http:目标解析到私有/保留地址,拒绝(SSRF 防线,v2) |

/// 内存越界 / 非法 UTF-8 / 值超限 / `__alloc` 缺失或失败。
pub const ERR_BOUNDS: i32 = -1;
/// data:记录或单插件配额超限。
pub const ERR_QUOTA: i32 = -6;
/// 数据库错误:kv_get / nodes_query / emit_event / data 系列。
pub const ERR_DB: i32 = -8;
/// 插件 http 目标落在私有/保留网段时的拒绝码。
pub const ERR_SSRF: i32 = -9;
