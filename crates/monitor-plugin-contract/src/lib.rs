//! monitor 的**宿主契约**:插件经名为 `host` 的 import 模块看到的 13 个宿主
//! 函数的唯一实现,以及一份能独立跑起来的最小宿主状态。
//!
//! # 为什么有这个 crate
//!
//! 插件的 ABI 门过去只比一个整数 `abi_version`,契约测试跑的是桩宿主:改宿主
//! 函数签名不 bump 版本,门全绿、桩测试全绿,真插件 instantiate 时 import
//! 签名不匹配,运行时炸。根因是"门不验证宿主行为"。
//!
//! 于是把 monitor 的 13 个 `func_wrap` 闭包搬到这个 crate,泛型化到
//! [`Host`] + [`HasScratch`] 两个 trait 上:
//!
//! - monitor 主 crate 的真宿主 [`PluginState`] 实现这两个 trait,把每个操作
//!   委托回 `Arc<App>`(见 `monitor::plugin::host`);
//! - 这个 crate 自带 [`ContractState`](内存 kv / plugin_data / 节点表 + 可换
//!   http 后端),插件仓在 release 时 `cargo test` 里 `linker::<ContractState>`
//!   真 instantiate 自己的 wasm 并驱动一条合成事件。
//!
//! **同一份闭包编译进两边**:签名、返回码、限额改一边,另一边编译不过。这就是
//! "契约与生产不漂移"的机制,不需要 build.rs 文本替换,也不需要人工同步。
//!
//! # 用法(plugin 仓的 contract 测试)
//!
//! ```no_run
//! use monitor_plugin_contract::{linker, ContractState};
//!
//! # fn main() -> anyhow::Result<()> {
//! let engine = wasmtime::Engine::default();
//! let module = wasmtime::Module::from_file(&engine, "target/wasm32-unknown-unknown/release/plugin.wasm")?;
//! let mut store = wasmtime::Store::new(&engine, ContractState::for_test("com.example.plugin"));
//! let instance = linker::<ContractState>(&engine)?.instantiate(&mut store, &module)?;
//! let on_event = instance.get_typed_func::<(i32, i32), i32>(&mut store, "on_event")?;
//! let payload = br#"{"node_id":1,"name":"edge-1","observed_at":0}"#;
//! let alloc = instance.get_typed_func::<(i32,), i32>(&mut store, "__alloc")?;
//! let ptr = alloc.call(&mut store, (payload.len() as i32,))?;
//! let mem = instance.get_memory(&mut store, "memory").unwrap();
//! mem.data_mut(&mut store)[ptr as usize..ptr as usize + payload.len()].copy_from_slice(payload);
//! assert_eq!(on_event.call(&mut store, (ptr, payload.len() as i32))?, 0);
//! # Ok(())
//! # }
//! ```
//!
//! # 边界
//!
//! 这个 crate **不含**任何 monitor 内部引用(monitor 的 `App` / `db` /
//! `notification_bus` 一个都不出现),也不依赖 monitor 主 crate。它只依赖
//! wasmtime/serde/serde_json/chrono/tracing/anyhow。
//!
//! 它也不含 SSRF 网段判定与 reqwest 请求构造——那些留在真宿主
//! (`monitor::plugin::host_funcs`),因为插件请求要走
//! `App::plugin_http`(不跟随重定向的那个 client)。契约替身的 http 出口由
//! [`ContractHttp`] 显式提供,默认 [`NoopHttp`] 全拒。

pub mod constants;
pub mod contract_state;
pub mod error_codes;
pub mod host;
pub mod host_linker;
pub mod http;
pub mod kv;

pub use constants::ABI_VERSION;
pub use contract_state::ContractState;
pub use host::{HasScratch, Host, HttpMethod, NodeInfo};
pub use host_linker::linker;
pub use http::{ContractHttp, HttpCall, MockHttp, NoopHttp};
pub use kv::{setting_key, ContractKv, ContractNodes};
