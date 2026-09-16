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

hub 支持用 Rust 编写的 WASM 通知插件：节点到期、agent 掉线/恢复等事件会
派发给所有订阅了该事件的已启用插件，插件在沙箱（wasmtime）里运行，只能通过
14 个宿主函数与外界交互——日志、时钟、键值存储、受限的 https 请求、节点只读
查询、事件发出以及自有的 key/value 数据存储。仓库内置两个完整可编译的参考
实现：[`plugins/tg-notify`](plugins/tg-notify)（Telegram 通知，订阅
宿主事件）和 [`plugins/finance-stats`](plugins/finance-stats)（财务统计，
自己发事件 + 面板页面 + 自清理）。

> ABI v1（`abi_version = 1`）已停用，仅 v2。

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
version = "0.2.0"
abi_version = 2                       # 必须为 2
subscribes = ["agent_offline", "agent_online", "plugin_expiry_soon"]

[tick]                                # 可选：声明每小时调一次 on_tick
[page]                                # 可选：声明面板里的自定义页面
title = "财务统计"                    # page.title 在面板导航上显示
[cleanup]                             # 可选：声明 on_cleanup，由面板「清理」按钮调用
```

| 字段 | 校验规则 |
|---|---|
| `plugin_id` | 非空、不含 `:`（它是 kv 命名空间 `plugin.<plugin_id>:<key>` 的分隔符）；重复的 `plugin_id` 上传被拒，升级需先删除旧版 |
| `name` / `version` | 非空 |
| `abi_version` | 必须为 `2` |
| `subscribes` | 订阅的宿主事件列表；v2 起宿主自身不再产生到期提醒，到期由财务类插件发 `plugin_expiry_soon`，其他插件订阅这条而不是旧的 `expiry_soon` |
| `wasm_entry` | 可选，缺省 `plugin.wasm`：包内 wasm 入口文件名 |
| `[tick]` | 存在则每 60 秒调度一次 `on_tick` |
| `[page]` | 存在则面板里出现「页面」入口；`title` 必填 |
| `[cleanup]` | 存在则面板里出现「清理」按钮，调 `on_cleanup` |

`subscribes` / `[tick]` / `[page]` / `[cleanup]` 至少要有一个——纯插件
不会有任何触达。等价于 v1 的「必须订阅到期/掉线/上线」三条之一。

### ABI v2 契约

模块 target 为 `wasm32-unknown-unknown`，`crate-type = ["cdylib"]`（std
可用，但没有网络/时间/文件等系统调用——一律走宿主函数）。

**必须导出**（缺失或签名不符在加载时被拒）：

| 导出 | 签名 | 用途 |
|---|---|---|
| `memory` | 线性内存 | 所有指针都落在它上面 |
| `on_event` | `(ptr: i32, len: i32) -> i32` | 事件入口；返回 0 成功，非 0 是插件自定义错误码 |
| `__alloc` | `(cap: i32) -> i32` | 分配器；宿主写事件载荷、`host_resp_alloc` 回程都走它（简单 bump 分配器即可） |

**按 manifest 声明按需导出**（少导则在加载时被拒，多导不会报错，但宿主
不会主动调用）：

| 导出 | 触发时机 |
|---|---|
| `on_tick()` | manifest 声明 `[tick]` 时，宿主每 60 秒调一次 |
| `render_page(ptr, len) -> i32` | manifest 声明 `[page]` 时，面板打开页面时调用，返回值为写入响应缓冲的字节数 |
| `on_action(ptr, len) -> i32` | manifest 声明 `[page]` 时，面板里的交互（按钮/表单提交）调用；与 `render_page` 一样的返回协议 |
| `on_cleanup(ptr, len) -> i32` | manifest 声明 `[cleanup]` 时，面板里的「清理」按钮调用 |

`render_page` 与 `on_action` 都按 JSON 协议工作：`host_resp_alloc` 拿缓冲、
写入一段 JSON、返回写入字节数；面板把这段 JSON 当 UI 描述渲染（详见
「面板页面协议」一节）。

**宿主函数**：从名为 `"host"` 的 wasm import 模块导入（Rust 侧用
`#[link(wasm_import_module = "host")]` + `#[link_name = "..."]`）：

| 函数 | 签名 | 返回值 / 说明 |
|---|---|---|
| `host_log` | `(level: i32, ptr, len)` | 无；level 0=debug 1=info 2=warn 3=error |
| `host_now` | `() -> i64` | 当前 Unix 秒 |
| `host_resp_alloc` | `(cap: i32) -> i32` | 宿主回调 `__alloc` 拿响应缓冲；之后 `host_http_post` / `host_http_get` 也可显式传 `resp_ptr > 0` 覆盖 |
| `host_http_post` | `(method_ptr, method_len, url_ptr, url_len, body_ptr, body_len, resp_ptr, resp_cap) -> i32` | 写 `Content-Type: application/json` 的 POST；错误码见下表 |
| `host_http_get` | `(url_ptr, url_len, resp_ptr, resp_cap) -> i32` | 固定 GET；同样见错误码表 |
| `host_kv_get` | `(key_ptr, key_len, out_ptr, out_cap) -> i32` | 写入 `out` 的字节数；**0 = 无值或空**；-1 越界/非法 UTF-8 |
| `host_kv_set` | `(key_ptr, key_len, val_ptr, val_len) -> i32` | 0 成功；-1 越界/值超 8 KiB；-2 写库失败 |
| `host_nodes_query` | `(out_ptr, out_cap) -> i32` | 把全部节点的精简快照（id/name/online/last_seen）写进缓冲；返回字节数或 -1 越界。仅供只读查询 |
| `host_emit_event` | `(name_ptr, name_len, payload_ptr, payload_len) -> i32` | 0 成功；-1 越界、-7 事件名不以 `plugin_` 开头、-8 内部错误。事件会经通知派发路径送达订阅者 |
| `host_data_put` | `(key_ptr, key_len, val_ptr, val_len) -> i32` | 写一行；value 上限 256 KiB、单插件总占用上限 16 MiB；超限 -6 |
| `host_data_get` | `(key_ptr, key_len, out_ptr, out_cap) -> i32` | 取一行；0 表示无此 key；-1 越界/非 UTF-8 |
| `host_data_delete` | `(key_ptr, key_len) -> i32` | 删一行；-1 越界 |
| `host_data_list` | `(prefix_ptr, prefix_len, out_ptr, out_cap) -> i32` | 按前缀列出 `[len:u32][key:len][key_bytes...]...` 的紧凑形式；-1 越界 |

错误码汇总：

| 码 | 含义 |
|---|---|
| -1 | 参数越界 / 非法 UTF-8 |
| -2 | URL 不是 `https://`（http_get / http_post 共用） |
| -3 | http_post 的 method 不是 `POST` |
| -4 | 网络请求失败 / 派发墙钟耗尽 |
| -5 | 响应状态非 2xx |
| -6 | plugin_data 超额（单行 / 单插件总占用） |
| -7 | emit_event 的事件名不以 `plugin_` 开头 |
| -8 | 数据库错误（emit_event 与 data 系列各自不同含义但共用此码） |

**事件载荷**：宿主经 `__alloc` 分配缓冲、写入事件 JSON（UTF-8），再调
`on_event(ptr, len)`。按字段名反序列化、容忍新增字段：

```json
{"type":"agent_offline","node_id":5,"name":"edge-1","observed_at":100,"last_seen_at":90}
{"type":"agent_online","node_id":5,"name":"edge-1","observed_at":300}
{"type":"plugin_expiry_soon","node_id":7,"name":"edge-1","expires_at":"2026-10-01","days_left":7,"threshold_days":7}
```

宿主事件只有 `agent_offline` / `agent_online` 两种 `type`，加上任意
`plugin_<作者选>` 由插件经 `host_emit_event` 发出。面板只识别
`agent_*` 与 `plugin_*` 前缀的事件。

### 面板页面协议（manifest 声明 `[page]` 时）

`render_page` 必须经 `host_resp_alloc` 拿一段缓冲、写入 JSON 描述、返回
写入字节数。`on_action` 也是同样的协议：body 是 `{action, ...}` 的 JSON，
返回新页面描述（操作完成后整页重渲染）。

返回 JSON 形如：

```json
{
  "title": "财务统计",
  "blocks": [
    {"type": "notice", "kind": "warning", "text": "汇率不可用……"},
    {"type": "stat", "items": [
      {"label": "年化续费总成本（USD）", "value": "128.40"}
    ]},
    {"type": "select", "name": "target_currency", "label": "展示币种",
     "value": "USD", "options": ["USD","CNY","EUR"],
     "action": "set_currency"},
    {"type": "table", "title": "7 天内到期（3）",
     "columns": ["节点", "到期日", "剩余天数"],
     "rows": [["edge-1","2026-10-01",3]]},
    {"type": "form", "title": "节点财务数据", "action": "save_node",
     "fields": ["name","price","currency","billing_cycle","expires_at"],
     "rows": [{"id":1,"name":"edge-1","price":12.5,"currency":"USD",
              "billing_cycle":"12","expires_at":"2027-01-01"}]}
  ]
}
```

支持的 `type`：`notice`（`kind: warning` 高亮，其余中性背景）、`stat`
（`items: [{label,value}]`）、`select`（提交 `{action, value}`）、
`table`（`rows: unknown[][]`）、`form`（`rows: {id, ...fields}`，提交
`{action, id, ...fields}`）。未知 `type` 被前端静默忽略，不报错。

### 资源限制

| 限制 | 值 | 说明 |
|---|---|---|
| fuel | 默认 1,000,000 指令/调用 | setting `plugin.fuel_limit` 可调；耗尽即中断（死循环被截断） |
| 墙钟 | 默认 5 秒/调用 | setting `plugin.timeout_ms` 可调 |
| kv 值 | 8 KiB | `host_kv_set` 与面板 KV 编辑器同限 |
| plugin_data 行 | 256 KiB | `host_data_put` 单行上限；超出返回 -6 |
| plugin_data 总占用 | 16 MiB / 插件 | 超额返回 -6 |
| http 响应 | resp 缓冲容量（自选） | 插件自己决定缓冲大小（如 4 KiB），超出部分截断 |
| 上传包 | 8 MiB | tar.gz 整包 |

### 上传与生命周期

1. 打包：`plugin.tar.gz` 内含 `plugin.toml` 与 `plugin.wasm`；
2. 上传：面板「插件」页，或 `POST /api/plugins`（multipart 字段 `plugin`）。
   上传时做预热校验（manifest 合法性、模块能编译、导出契约齐全），失败原因
   写进插件状态供面板查看；上传后默认**不启用**；
3. 启用：`POST /api/plugins/{id}/enable`（加载失败会标记 `failed` 并带原因）；
4. 测试：`POST /api/plugins/{id}/test` 构造一条合成的 `plugin_expiry_soon`
   事件，走与真实派发完全相同的执行路径；
5. 页面：`GET /api/plugins/{id}/page` 调 `render_page` 拿 JSON 描述；
   面板里的交互走 `POST /api/plugins/{id}/action` 调 `on_action`；
6. 清理：`POST /api/plugins/{id}/cleanup` 调 `on_cleanup`——清理逻辑
   完全在插件手里，宿主只转发调用与回收统计；
7. 日志：`GET /api/plugins/{id}/logs` 返回最近 100 条派发结果（进程内环形
   缓冲，重启后为空；长期审计在 notification_log）。

渠道配置（bot token 等）不建议打进 wasm——写在面板的插件 KV 编辑器里
（`PUT /api/plugins/{id}/kv/{key}`），插件运行时用 `host_kv_get` 读取。
插件自有数据请走 `host_data_*`——这两套互不相通。

### 已知约束

- **失败不会自动禁用**：连续失败的插件不会被自动停用，需操作员手动
  disable，或修复后删除再重新 upload；
- **不做签名校验**：hub 不验证 wasm 的来源，任何人拿到管理员会话即可上传
  任意插件（插件能读自己的 kv、向任意 https 地址发 POST/GET、能发
  `plugin_*` 事件）。只上传自己编译的 wasm；
- **无重试语义**：事件派发给插件一次，失败不重发；同一事件的重复通知由
  hub 侧的幂等机制抑制，与插件无关。
- **节点财务字段**：自 v2 起宿主 node 表不再读取价格/币种/周期/到期日
  这些字段，它们只在引用了财务插件的部署里以 `plugin_data` 行存在。新装
  的节点表里这四列仍然存在（schema 兼容，未使用的列），会在后续版本里
  走迁移删掉。

