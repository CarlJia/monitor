# monitor-plugin-contract

monitor 的**宿主契约**:插件经名为 `host` 的 import 模块看到的 13 个宿主函数的
**唯一实现**,以及一份能独立跑起来的最小宿主状态。

> monitor 仓 `docs/` 被 gitignore,所以这份文档放在 crate 内(随 crate 走),
> 而不是 plan 原先写的 `docs/plugin-contract.md`。

## 为什么有这个 crate

插件的 ABI 门过去只比一个整数 `abi_version`,契约测试跑的是每个插件仓里**手写的
桩宿主**。于是:改 monitor 的宿主函数签名但不 bump `ABI_VERSION` → 所有门全绿、
桩测试也全绿,真实插件在 `instantiate` 时因 import 签名不匹配而挂。根因是
**门不验证宿主行为**。

修法:把 monitor 的 13 个 `func_wrap` 闭包搬进这个 crate,泛型化到两个 trait 上。
**同一份闭包编译进两边**——签名、返回码、限额改一边,另一边编译不过。不需要
build.rs 文本替换,也不需要人工同步。

## 两个 trait

- `Host` —— 10 个带宿主副作用的方法:`kv_get`/`kv_set`、`data_put`/`data_get`/
  `data_delete`/`data_list`、`http_request`、`nodes_query`、`emit_event`。
- `HasScratch` —— 调用期状态:`plugin_id`、`deadline`、`resp`(`resp_ptr`/`resp_cap`)、
  `push_log`。

两边实现:

- **monitor 主 crate** 的真宿主 `PluginState` 实现这两个 trait,把每个操作委托回
  `Arc<App>`(见 `monitor/src/plugin/host.rs`)。
- **本 crate** 自带 `ContractState`:内存 kv / plugin_data / 节点表 + 可换 http 后端
  (默认 `NoopHttp` 全拒,测试用 `MockHttp::respond_with`)。

`now` / `log` / `resp_alloc` 不碰 `App`,只用 `HasScratch`。

## 常量是单源

限额与错误码(`DEFAULT_FUEL_LIMIT`、`DEFAULT_HOOK_FUEL_LIMIT`、`KV_VALUE_MAX`、
`KV_KEY_MAX`、`HTTP_TIMEOUT`、`HTTP_RESP_MAX`、`RECORD_MAX`、`PLUGIN_DATA_MAX`、
`ERR_*`)在本 crate 定义;monitor 主 crate `pub use` 它们。改一处,另一处不必跟。

## plugin 端怎么用

插件仓的 `release.yml` 在 `./build.sh` 之后、发布之前,按本插件 `plugin.toml` 的
`abi_version` 拉对应主版本的 contract crate,跑 `tests/contract.rs`:

```rust
use monitor_plugin_contract::{linker, setting_key, ContractState, MockHttp};

let engine = wasmtime::Engine::default();
let wasm = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/plugin.wasm")).unwrap();
let module = wasmtime::Module::new(&engine, &wasm).unwrap();

let state = ContractState::for_test("com.example.my-plugin")   // = 本插件 plugin_id
    .with_http(Box::new(MockHttp::respond_with(200, b"{\"ok\":true}")));
state.kv().set(&setting_key("com.example.my-plugin", "some_key"), "value");

let mut store = wasmtime::Store::new(&engine, state);
let instance = linker::<ContractState>(&engine).unwrap()
    .instantiate(&mut store, &module)     // ← import 签名不匹配在这里就失败
    .expect("真实宿主的 import 签名必须与本插件一致");
// …再把事件载荷写进内存并调 on_event / on_tick，断言返回 0 / 不 trap。
```

## 版本与同步

- crate `version` 的**主版本号 == monitor `src/plugin/manifest.rs` 的 `ABI_VERSION`**;
  monitor CI 有断言守这条(见 `.github/workflows/ci.yml` 的 `contract` job)。
- 发布形态是 **git tag** `monitor-plugin-contract-v<X.Y.Z>`(不发 crates.io),
  plugin 仓按 `--tag` 拉。tag 校验见 `.github/workflows/publish-contract.yml`。
- 改宿主 ABI 的流程:改 `host_funcs.rs` → 若 `Host`/`HasScratch` 面变了,本 crate
  编译会失败(闭包对不上) → 同步实现 → 若破坏兼容则 bump `ABI_VERSION` 与本 crate
  主版本 → 推 `monitor-plugin-contract-v<新版本>` tag。

## 边界

本 crate **不含**任何 monitor 内部引用(`App`/`db`/`notification_bus` 一个都不出现),
也不依赖 monitor 主 crate;只依赖 wasmtime / serde / serde_json / chrono / tracing /
anyhow。SSRF 网段判定与 reqwest 请求构造**不在**这里——它们留在真宿主,因为插件请求
要走 `App::plugin_http`(不跟随重定向的那个 client);契约替身的 http 出口由
`ContractHttp` 显式提供。
