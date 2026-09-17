//! WASM plugin runtime.
//!
//! - [`manifest::Manifest::parse`] 校验 manifest(R7),
//! - [`host::load`] 把 db 行变成 [`host::LoadedPlugin`](R6 的前半段:编译 + 导出契约检查),
//! - [`host::call_on_event`] 每次事件新建 Store、注入宿主函数、以 fuel 限额调用插件,
//! - 6 个宿主函数(R8),
//! - [`Registry`] 启动预加载 enabled 插件(R10)、按 manifest.subscribes 派发
//!   (R5)、以超时/fuel 隔离每个插件(R9)、维护 dispatch_log 环形缓冲(R16)
//!   并回写 notification_log(R14)。
//!
//! 本模块按关注点拆成五个子模块;外部消费面(加载、引擎、注册表、Manifest、
//! KV 上限)在此处重新导出,外部路径不变:
//!
//! - [`manifest`] — Manifest 结构与校验(R7);
//! - [`host`] — 引擎、加载、调用骨架与事件派发入口(R6/R9);
//! - [`host_funcs`] — 14 个宿主函数(R8);
//! - [`log`] — 一次调用里的插件日志汇集点(R16 的 `detail`);
//! - [`registry`] — 注册表、派发、隔离与回写(U4)。
//!
//! # wasm 模块契约(U8 的示例插件按此实现)
//!
//! 模块必须导出:
//!
//! | 导出 | 签名 | 用途 |
//! |------|------|------|
//! | `memory` | 线性内存 | 宿主函数的指针都落在它上面 |
//! | `on_event` | `(ptr: i32, len: i32) -> i32` | 事件入口;入参指向 JSON 载荷,返回 0 表示成功,非 0 是插件自定义错误码 |
//! | `__alloc` | `(cap: i32) -> i32` | 分配器;宿主写载荷前通过它拿缓冲(`host_resp_alloc` 同样回调它) |
//!
//! 模块从 `"host"` 模块导入宿主函数(名字与返回值见 [`host`] 里各函数的文档;错误码统一为负数,
//! 成功时 kv_get/http_post 返回写入的字节数,其余返回 0)。
//!
//! 事件载荷是 [`crate::notification_bus::Event`] 的 JSON,形如
//! `{"type":"expiry_soon","node_id":7,...}`——按字段名反序列化、容忍新增字段。
//!
//! 资源模型(A8):引擎进程唯一(见 [`host::new_engine`]),`LoadedPlugin` 只缓存 manifest
//! 与 `Module`(均 Send+Sync);实例与 Store 每次调用重建——fuel 记在 Store 上,
//! 复用会让首次耗尽 fuel 的插件永久死亡,也无法并发调用。
//!
//! 每次调用另建一个日志汇集点(`log::PluginLog`),和 Store 一起生、一起灭,
//! 但由调用方另持一份 `Arc`:插件经 `host.log` 打的话要跟着派发结果回面板,
//! 而 Store 在返回时已经没了;超时被放弃的那个任务更是只剩调用方手里这一份
//! 才读得到「放弃前它说了什么」。

mod host;
mod host_funcs;
mod log;
mod manifest;
mod registry;

pub use host::{kv_key_problem, load, new_engine, KvKeyProblem, KV_KEY_MAX, KV_VALUE_MAX};
pub use manifest::Manifest;
pub use registry::Registry;

#[cfg(test)]
pub(crate) mod test_util;

#[cfg(test)]
pub(crate) use test_util::MINIMAL_WAT;
