# monitor

## 特性

- 实时监控：秒级实时数据展示
- 轻量高效：Rust 语言构建，低资源占用，极简高效
- 自托管：完全掌控数据隐私，部署简单

## 组成

| 仓库 | 说明 |
|---|---|
| [monitor](https://github.com/monitor-probe/monitor) | hub：后台、API、公开页宿主 |
| [agent](https://github.com/monitor-probe/agent) | Linux agent |
| [monitor-theme-default](https://github.com/monitor-probe/monitor-theme-default) | 内置默认主题 |

```
agent (Linux)  ──WebSocket / JSON-RPC 2.0──▶  hub (axum + SQLite)  ──▶  后台 + 状态页
```

## 插件开发

hub 支持用 Rust 编写的 WASM 通知插件：节点到期、agent 掉线/恢复等事件会被
派发给所有订阅了该事件的已启用插件，插件在沙箱（wasmtime）里运行，只能通过
6 个宿主函数与外界交互——日志、时钟、键值存储和受限的 https POST。仓库内
置一个完整可编译的参考实现：[`plugins/tg-notify`](plugins/tg-notify)
（Telegram 通知）。

### 快速开始

```sh
cd plugins/tg-notify
rustup target add wasm32-unknown-unknown   # 一次性
./build.sh                                  # 产出 plugin.tar.gz
cargo test                                  # 桩宿主冒烟测试
```

把它当模板复制一份，改 `plugin_id` 为你自己的反向域（如
`io.github.<用户名>.my-notify`）即可。

### plugin.toml（manifest）

```toml
plugin_id = "com.example.tg-notify"   # 反向域风格；不能为空、不能含 ':'
name = "Telegram 通知"                # 面板里显示的名字
version = "0.1.0"
abi_version = 1                       # 必须等于 1
subscribes = ["expiry_soon", "agent_offline", "agent_online"]
```

| 字段 | 校验规则 |
|---|---|
| `plugin_id` | 非空、不含 `:`（它是 kv 命名空间 `plugin.<plugin_id>:<key>` 的分隔符）；重复的 `plugin_id` 上传被拒，升级需先删除旧版 |
| `name` / `version` | 非空（version v1 只做非空校验） |
| `abi_version` | 必须为 `1` |
| `subscribes` | 至少一项，且只能是 v1 的三个事件名：`expiry_soon`、`agent_offline`、`agent_online` |
| `wasm_entry` | 可选，缺省 `plugin.wasm`：包内 wasm 入口文件名 |

### ABI v1 契约

模块 target 为 `wasm32-unknown-unknown`，`crate-type = ["cdylib"]`（std 可用，
但没有网络/时间等系统调用——一律走宿主函数）。

**必须导出**（缺失或签名不符在加载时被拒）：

| 导出 | 签名 | 用途 |
|---|---|---|
| `memory` | 线性内存 | 所有指针都落在它上面 |
| `on_event` | `(ptr: i32, len: i32) -> i32` | 事件入口；返回 0 成功，非 0 是插件自定义错误码 |
| `__alloc` | `(cap: i32) -> i32` | 分配器；宿主写事件载荷、`host_resp_alloc` 回程都走它（简单 bump 分配器即可） |

**宿主函数**：从名为 `"host"` 的 wasm import 模块导入（Rust 侧用
`#[link(wasm_import_module = "host")]` + `#[link_name = "..."]`）：

| 函数 | 签名 | 返回值 |
|---|---|---|
| `host_log` | `(level: i32, ptr, len)` | 无；level 0=debug 1=info 2=warn 3=error |
| `host_now` | `() -> i64` | 当前 Unix 秒 |
| `host_http_post` | `(method_ptr, method_len, url_ptr, url_len, body_ptr, body_len, resp_ptr, resp_cap) -> i32` | 写入 resp 的字节数；负数见错误码表 |
| `host_kv_get` | `(key_ptr, key_len, out_ptr, out_cap) -> i32` | 写入 out 的字节数；**0 = 无值或空**；-1 越界/非法 UTF-8 |
| `host_kv_set` | `(key_ptr, key_len, val_ptr, val_len) -> i32` | 0 成功；-1 越界/值超 8 KiB；-2 写库失败 |
| `host_resp_alloc` | `(cap: i32) -> i32` | 宿主回调你的 `__alloc` 拿响应缓冲；也可以自己 `__alloc` 后把指针直接传给 `http_post`，效果相同 |

`host_http_post` 固定发 `Content-Type: application/json` 的 POST，单请求
4 秒超时。错误码：

| 码 | 含义 |
|----|------|
| -1 | 参数越界 / 非法 UTF-8 / resp 写不进 |
| -2 | URL 不是 `https://` |
| -3 | method 不是 `POST` |
| -4 | 网络请求失败 |
| -5 | 响应状态非 2xx |

**事件载荷**：宿主经 `__alloc` 分配缓冲、写入事件 JSON（UTF-8），再调
`on_event(ptr, len)`。按字段名反序列化、容忍新增字段，`type` 是判别标签：

```json
{"type":"expiry_soon","node_id":7,"name":"edge-1","expires_at":"2026-10-01","days_left":7,"threshold_days":7}
{"type":"agent_offline","node_id":5,"name":"edge-1","observed_at":100,"last_seen_at":90}
{"type":"agent_online","node_id":5,"name":"edge-1","observed_at":300}
```

### 资源限制

| 限制 | 值 | 说明 |
|---|---|---|
| fuel | 默认 1,000,000 指令/调用 | setting `plugin.fuel_limit` 可调；耗尽即中断（死循环被截断） |
| 墙钟 | 默认 5 秒/调用 | setting `plugin.timeout_ms` 可调 |
| kv 值 | 8 KiB | `host_kv_set` 与面板 KV 编辑器同限 |
| http 响应 | resp 缓冲容量（自选） | 插件自己决定缓冲大小（如 4 KiB），超出部分截断 |
| 上传包 | 8 MiB | tar.gz 整包 |

### 上传与生命周期

1. 打包：`plugin.tar.gz` 内含 `plugin.toml` 与 `plugin.wasm`；
2. 上传：面板「插件」页，或 `POST /api/plugins`（multipart 字段 `plugin`）。
   上传时做预热校验（manifest 合法性、模块能编译、导出契约齐全），失败原因
   写进插件状态供面板查看；上传后默认**不启用**；
3. 启用：`POST /api/plugins/{id}/enable`（加载失败会标记 `failed` 并带原因）；
4. 测试：`POST /api/plugins/{id}/test` 构造一条合成的 `expiry_soon` 事件，
   走与真实派发完全相同的执行路径；
5. 日志：`GET /api/plugins/{id}/logs` 返回最近 100 条派发结果（进程内环形
   缓冲，重启后为空；长期审计在 notification_log）。

渠道配置（bot token 等）不建议打进 wasm：写在面板的插件 KV 编辑器里
（`PUT /api/plugins/{id}/kv/{key}`），插件运行时用 `host_kv_get` 读取。

### v1 已知局限

- **失败不会自动禁用**：连续失败的插件不会被自动停用，需操作员手动
  disable，或修复后删除再重新 upload；
- **不做签名校验**：hub 不验证 wasm 的来源，任何人拿到管理员会话即可上传
  任意插件（插件能读自己的 kv、向任意 https 地址发 POST）。只上传自己编译
  的 wasm；
- **无重试语义**：事件派发给插件一次，失败不重发；同一事件的重复通知由
  hub 侧的幂等机制抑制，与插件无关。

