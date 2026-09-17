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

> **升级到 2.0.0 的破坏性变更**：ABI v1（`abi_version = 1`）已停用，仅 v2
> ——已安装的 v1 插件**不再加载**，需对着 v2 重编并重新上传。同时：节点 JSON
> 去掉 `price`/`currency`/`billing_cycle`/`expires_at` 四个字段、
> `notification.expiry_thresholds` 设置不再可读、宿主内置的到期提醒退役
> （改由财务插件发 `plugin_expiry_soon`）。未安装财务插件的部署不再有到期通知。
> 旧库里的那四列由启动迁移删除（需先启用财务插件完成导入），旧值不会被迁移。

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

[[kv]]                            # 可选、可重复：面板「配置」对话框要展示的 kv 字段
key = "bot_token"                     # kv 的 key，必须与插件里 host_kv_get 读的名字逐字一致
label = "Telegram Bot Token"          # 显示用的人话名字；可省，省了只显示 key
required = true                       # 点「测试」前必须有值；可省，缺省 false
hint = "向 @BotFather 申请"            # 一句话填写提示；可省
```

| 字段 | 校验规则 |
|---|---|
| `plugin_id` | 非空、不含 `:`（它是 kv 命名空间 `plugin.<plugin_id>:<key>` 的分隔符）；重复的 `plugin_id` 上传被拒，升级需先删除旧版 |
| `name` / `version` | 非空 |
| `abi_version` | 必须为 `2` |
| `subscribes` | 订阅的宿主事件列表，最多 32 条；v2 起宿主自身不再产生到期提醒，到期由财务类插件发 `plugin_expiry_soon`，其他插件订阅这条而不是旧的 `expiry_soon` |
| `wasm_entry` | 可选，缺省 `plugin.wasm`：包内 wasm 入口文件名 |
| `[tick]` | 存在则每小时调度一次 `on_tick` |
| `[page]` | 存在则面板里出现「页面」入口；`title` 必填 |
| `[cleanup]` | 存在则面板里出现「清理」按钮，调 `on_cleanup` |
| `[[kv]]` | 可选、可重复，最多 64 项。key 非空、不含 `:`、不超 128 字节、不重复、首尾无空白（形状规则由 `plugin::kv_key_problem` 单点判定，面板的 kv 写入同一份）；`required` 的含义只是「点『测试』前应该有值」，**宿主在真实派发里从不检查它**——后台事件旁边没有操作员，一个 400 也无处可给 |

`subscribes` / `[tick]` / `[page]` / `[cleanup]` 至少要有一个——纯插件
不会有任何触达。等价于 v1 的「必须订阅到期/掉线/上线」三条之一；只声明
`[[kv]]` 不算工作面（它只是面板的展示与预检）。

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
| `on_tick()` | manifest 声明 `[tick]` 时，宿主每小时调一次 |
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
| `host_log` | `(level: i32, ptr, len)` | 无；level 0=debug 1=info 2=warn 3=error。文本除进 hub 日志外，还按有界缓冲捕获，随该次派发的 `detail` 出现在面板「派发日志」与「测试」结果里（见「资源限制」） |
| `host_now` | `() -> i64` | 当前 Unix 秒 |
| `host_resp_alloc` | `(cap: i32) -> i32` | 宿主回调 `__alloc` 拿响应缓冲；之后 `host_http_post` / `host_http_get` 也可显式传 `resp_ptr > 0` 覆盖 |
| `host_http_post` | `(method_ptr, method_len, url_ptr, url_len, body_ptr, body_len, resp_ptr, resp_cap) -> i32` | 写 `Content-Type: application/json` 的 POST；错误码见下表 |
| `host_http_get` | `(url_ptr, url_len, resp_ptr, resp_cap) -> i32` | 固定 GET；同样见错误码表 |
| `host_kv_get` | `(key_ptr, key_len, out_ptr, out_cap) -> i32` | 写入 `out` 的字节数；**0 = 无值或空**；-1 越界/非法 UTF-8；-8 读库失败（与 0 分开，插件才能区分「没配」与「库坏了」） |
| `host_kv_set` | `(key_ptr, key_len, val_ptr, val_len) -> i32` | 0 成功；-1 越界/值超 8 KiB；-2 写库失败 |
| `host_nodes_query` | `(out_ptr, out_cap) -> i32` | 把全部节点的精简快照（id/name/online）写进缓冲；返回字节数、-1 越界或 -6 放不下。仅供只读查询 |
| `host_emit_event` | `(name_ptr, name_len, payload_ptr, payload_len) -> i32` | 0 成功；-1 越界、-7 事件名不以 `plugin_` 开头、-8 内部错误。事件会经通知派发路径送达订阅者 |
| `host_data_put` | `(key_ptr, key_len, val_ptr, val_len) -> i32` | 写一行；value 上限 256 KiB、单插件总占用上限 16 MiB；超限 -6 |
| `host_data_get` | `(key_ptr, key_len, out_ptr, out_cap) -> i32` | 取一行；0 表示无此 key；-1 越界/非 UTF-8 |
| `host_data_delete` | `(key_ptr, key_len) -> i32` | 删一行；-1 越界 |
| `host_data_list` | `(prefix_ptr, prefix_len, out_ptr, out_cap) -> i32` | 按前缀列出，JSON 数组 `[{"key":"node:1","data":"..."},...]`；返回字节数、-1 越界或 -6 放不下 |

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
| -8 | 数据库错误（kv_get / nodes_query / emit_event / data 系列共用此码，各自场景不同） |
| -9 | http 目标解析到私有/保留网段，拒绝（SSRF 防线） |

`host_http_post` / `host_http_get` 只允许访问公网 https 地址：私有段
（10/8、172.16/12、192.168/16）、回环、链路本地与云元数据
（169.254.169.254）、CGNAT、保留段，以及解析后落进这些网段的主机名，一律
按 -9 拒绝。插件请求**不跟随重定向**（3xx 表现为 -5）——预检只看得到首个 URL，跟随会让任一公网开放重定向绕过它。这是**插件的**限制——宿主自身的 http 调用（主题、GitHub、运维
配置的 `github_proxy` 镜像）不经过这层，内网镜像照常可用。防线在 DNS 解析
后判定，但解析与发送之间仍存在 DNS rebinding 的时间窗；威胁模型是「管理员
安装的插件」，真正的隔离靠 wasm 沙箱的其余边界。

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

`render_page` 允许带一次性副作用（例如尚无缓存时顺手拉一次汇率），但它在
每次打开页面和每次刷新时都会被调用，所以这类工作必须**幂等且有界**——面板
与页面上的刷新按钮会反复调它，副作用不能累积、也不能随调用次数增长。

返回 JSON 形如：

```json
{
  "title": "财务统计",
  "toast": {"kind": "success", "text": "已保存"},
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
     "fields": [
       {"name": "name", "label": "节点名", "type": "text"},
       {"name": "price", "label": "价格", "type": "number"},
       {"name": "currency", "label": "币种", "type": "select", "options": ["CNY","USD"]},
       {"name": "billing_cycle", "label": "计费周期", "type": "select",
        "options": [{"value":"monthly","label":"月付"},{"value":"once","label":"一次性"}]},
       {"name": "expires_at", "label": "到期日", "type": "date"}
     ],
     "rows": [{"id":1,"name":"edge-1","price":12.5,"currency":"USD",
               "billing_cycle":"monthly","expires_at":"2027-01-01"}]}
  ]
}
```

支持的 `type`：`notice`（`kind: warning` 高亮，其余中性背景）、`stat`
（`items: [{label,value}]`）、`select`（提交 `{action, value}`；这个级别的
`options` 仍是字符串数组，显示即提交，值本身需要中文文案时改用 `form` 行里的
`select` 字段）、`table`（`rows: unknown[][]`）、`form`（`rows: {id, ...fields}`，
提交 `{action, id, ...fields}`）。未知 `type` 被前端静默忽略，不报错。

顶层可选的 `toast` 是**操作回执**：`{"kind": …, "text": …}`，面板在
`on_action` 的响应到达时弹一次，文案由插件给（宿主不替插件编文案）。`kind`
取 `success` / `error` / `info` / `warning`，省略或写了别的值按 `success`
处理；`text` 为空（或没有 `toast`）就不弹。**初始 `render_page` 的响应不触发
提示**——否则每次打开页面都会重播上一次操作的结果。失败分支也走这条路：插件
把原因写进 `toast.text`（`kind: "error"`）比只把状态码塞进页面更直接。

`form` 的 `fields` 有两种形态，都接受：

```json
"fields": ["name", "price", "currency", "billing_cycle", "expires_at"]
"fields": [{"name": "price", "label": "价格", "type": "number"},
           {"name": "currency", "label": "币种", "type": "select",
            "options": ["CNY", "USD"]},
           {"name": "billing_cycle", "label": "计费周期", "type": "select",
            "options": [{"value": "monthly", "label": "月付"},
                        {"value": "once", "label": "一次性"}]}]
```

- `name`（必填）是提交载荷里的键，也是取值时的键；没有名字的条目会被丢弃。
- `label` 是编辑表的列头文案，缺省（或全是空白）时用字段名——旧式声明因此
  照旧显示字段标识，新式声明才能显示「节点名」这类中文表头。
- `type` 取 `text` / `number` / `date` / `select`。**声明优先**：写了
  `type: "number"` 的字段即使名字叫 `fee` 也会渲染成数字输入框、并按数字
  提交。
- `select` 需要一并给 `options`，面板渲染成下拉，选中值写进该行草稿、随该行
  的「保存」一起提交。**没给 `options` 的 `select` 会回退**到下面的字段名
  启发式——一个没有可选项的下拉是死控件，既改不了也清不掉。
- `options` 里每一项可以是**裸字符串**（既是提交值也是显示文案），也可以是
  **`{"value": …, "label": …}` 对象**：面板显示 `label`、提交 `value`。值本身
  不是人话的字段（`monthly` / `once` / 币种代码）用它把标识与文案分开——
  `label` 缺省或全是空白时回退成 `value`，`value` 不是非空字符串的项直接
  丢掉（渲染出来要么是死选项要么是看不见的选项）。**提交的永远是 `value`**，
  面板不会把 `label` 写回载荷。
- `type` 缺省或认不出（比如写成 `currency`、`int`）同样回退到字段名启发式:
  插件写错一个词不该让整列变成不能用的控件。

`label` 是给操作者看的，`name` 才是协议；两者不一致时以 `name` 为准。

`select` 表达不了"清空/未设置"：`options` 里没有空值项，Radix 的触发器也不会
把选择退回去，所以下拉**只能改、不能清**。需要让操作员清掉某个字段的插件
（比如把计费周期恢复成未填写）应当把该字段声明成文本字段——文本框清空提交
空串是明确表达的。这是刻意的收窄：加一个空选项会让"选中空值"和"还没选过"
在下拉里长得一模一样。

旧式字段名启发式（`fields` 没给 `type` 时）仍是**协议的一部分**（不是实现
细节）：名字里含 `price`/`cost`/`amount`（不分大小写）用数字输入框，以 `at`
结尾的用日期输入框，其余用文本框。字段名不合这套规则就会拿到错误的控件
（比如把价格叫 `unit_cost_value` 仍会被认成数字，但叫 `fee` 就只会是文本框
——新式声明写 `type` 才治本）。

数字字段清空表示"不改这个字段"，不会存成 0——`Number("")` 是 0，而 0 在这类
字段里通常有实际含义（比如财务插件的 0 表示免费）。非空但解析不出数字的
（如 `12,5`）也一样跳过，保留服务端原值。行内控件与「保存」按钮共用同一把
锁：一次 action 在途时整表禁用，否则响应回来重挂载表单会静默丢弃这几秒里的
改动。

### 资源限制

| 限制 | 值 | 说明 |
|---|---|---|
| fuel（事件派发） | 默认 1,000,000 指令/调用 | setting `plugin.fuel_limit` 可调；耗尽即中断（死循环被截断） |
| fuel（数据面钩子） | 默认 20,000,000 指令/调用 | setting `plugin.hook_fuel_limit` 可调；`on_tick`/`render_page`/`on_action`/`on_cleanup` 用这一档——它们的开销随插件自己的数据规模增长，`render_page` 还可能带一次性副作用（如拉一次汇率，见「面板页面协议」），按有界事件载荷定的派发那档不够用；财务插件的页面要列出全部节点：实测空页面 44 万 fuel、每台机器再 5.4 万，tick 是 67 万 + 每台 2.4 万 |
| 墙钟 | 默认 5 秒/调用 | setting `plugin.timeout_ms` 可调 |
| kv 值 | 8 KiB | `host_kv_set` 与面板 KV 编辑器同限 |
| plugin_data 行 | 256 KiB | `host_data_put` 单行上限；超出返回 -6 |
| plugin_data 总占用 | 16 MiB / 插件 | 超额返回 -6 |
| http 响应 | resp 缓冲容量（自选） | 插件自己决定缓冲大小（如 4 KiB），超出部分截断 |
| 插件日志 detail | 16 行 / 每行 200 字节（截断时另加省略号）/ 合计 500 字节 | 一次调用里插件经 `host_log` 打的话，取**最新**，超出的更早行丢弃并在开头标注。换行、行分隔符与双向控制符等不可见字符一律折成空格——detail 是按行显示的，多行文案会被压成一行。超时的条目取的是超时那一刻的快照：被放弃的任务之后仍会继续写，但那部分不会进 detail |
| 上传包 | 8 MiB | tar.gz 整包 |

### 上传与生命周期

1. 打包：`plugin.tar.gz` 内含 `plugin.toml` 与 `plugin.wasm`；
2. 上传：面板「插件」页，或 `POST /api/plugins`（multipart 字段 `plugin`）。
   上传时做预热校验（manifest 合法性、模块能编译、导出契约齐全），失败原因
   写进插件状态供面板查看；上传后默认**不启用**；
3. 启用：`POST /api/plugins/{id}/enable`（加载失败会标记 `failed` 并带原因）；
4. 测试：`POST /api/plugins/{id}/test` 构造一条合成的 `plugin_expiry_soon`
   事件，走与真实派发完全相同的执行路径。派发前先按 manifest 的 `[[kv]]`
   预检必填项，缺项（行不存在或值为空）直接 400 点名缺哪一项，而不是让插件
   回来一个 `other:2` 让操作员猜；
5. 页面：`GET /api/plugins/{id}/page` 调 `render_page` 拿 JSON 描述；
   面板里的交互走 `POST /api/plugins/{id}/action` 调 `on_action`；
6. 清理：`POST /api/plugins/{id}/cleanup` 调 `on_cleanup`——清理逻辑
   完全在插件手里，宿主只转发调用与回收统计；
7. 日志：`GET /api/plugins/{id}/logs` 返回最近 100 条派发结果（进程内环形
   缓冲，重启后为空；长期审计在 notification_log）。每条另带 `detail`：插件
   自己经 `host.log` 打的话——`other:2` 这种错误码是插件私有的，原因只可能
   在那句话里。`detail` 只进内存派发日志，不进 notification_log（那是宿主的
   审计表，不混插件自由文本）。

渠道配置（bot token 等）不建议打进 wasm——在 manifest 里用 `[[kv]]` 声明
字段（面板据此渲染标签、必填标记与提示），值写在面板的插件 KV 编辑器里
（`PUT /api/plugins/{id}/kv/{key}`），插件运行时用 `host_kv_get` 读取。
插件自有数据请走 `host_data_*`——这两套互不相通。

### 已知约束

- **失败不会自动禁用**：连续失败的插件不会被自动停用，需操作员手动
  disable，或修复后删除再重新 upload；
- **不做签名校验**：hub 不验证 wasm 的来源，任何人拿到管理员会话即可上传
  任意插件（插件能读自己的 kv、向任意**公网** https 地址发 POST/GET、能发
  `plugin_*` 事件）。私有/保留网段被 SSRF 防线挡下（见错误码 -9），但插件
  仍可与任意公网主机通信。只上传自己编译的 wasm；
- **无重试语义**：事件派发给插件一次，失败不重发；同一事件的重复通知由
  hub 侧的幂等机制抑制，与插件无关。
- **节点财务字段**：自 v2 起宿主 node 表不再持有价格/币种/周期/到期日——
  它们归财务插件的 `plugin_data`。旧库里的这四列由升级迁移删除，但**分两段**：
  只有当财务插件已启用并把节点导入自己的存储后，下一次启动才执行删列；
  「先换二进制、暂不装插件」的部署会一直保留这四列（闲置，宿主与插件都不读）。
  升级用户此前录入的价格/周期/到期日不会被迁移到插件里——需要的话，启用插件
  后在「财务统计」页面重新录入。

