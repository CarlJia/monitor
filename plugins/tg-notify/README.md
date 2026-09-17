# tg-notify

monitor-hub 的内置示例通知插件，wasm 插件 ABI v2 的参考实现——订阅宿主的
离线/在线事件与财务插件发出的 `plugin_expiry_soon`，把通知渲染成中文
文案，经 Telegram Bot API 的 `sendMessage` 发出。完整的 ABI 文档见仓库根
README 的「插件开发」章节，权威实现见 `src/plugin/`。

## 行为

| 事件 | 文案 |
|---|---|
| `plugin_expiry_soon` | ⏰ 节点 {name} 将于 {expires_at} 到期（剩 {days_left} 天） |
| `agent_offline` | 🔴 节点 {name} 已离线（最后上报于 N 秒前） |
| `agent_online` | 🟢 节点 {name} 已恢复在线 |

> ABI v2 起宿主的到期检测退役，`expiry_soon` 事件由财务统计插件经
> `emit_event` 发出，事件名带 `plugin_` 前缀。未启用财务插件的部署不再有
> 到期通知。

`on_event` 返回码：

| 码 | 含义 |
|----|------|
| 0 | 成功 |
| 1 | 事件 JSON 解析失败 |
| 2 | kv 里没有 `bot_token` |
| 3 | kv 里没有 `chat_id` |
| 11–19 | http 失败（10 减去宿主错误码：11=越界 12=非 https 13=非 POST 14=网络 15=非 2xx 19=目标为私有/保留地址被拒） |

这些码对操作员没有可读性，所以 `plugin.toml` 用 `[[kv]]` 声明了两个 kv
字段；面板据此渲染「配置」对话框，并在点「测试」前预检必填项——缺项直接
400 点名，不会再让你对着 `other:2` 猜。真的派发失败时，插件经 `host.log`
打的那句话（如「kv 里没有 bot_token；请在面板的插件 KV 编辑器里填写」）会随
结果一起回面板，显示在「测试」的提示与「派发日志」里。

## 构建

需要一次性的目标安装：

```sh
rustup target add wasm32-unknown-unknown
```

然后：

```sh
./build.sh
```

产出 `plugin.tar.gz`（内含 `plugin.toml` + `plugin.wasm`）。

冒烟测试（用 wasmtime 桩宿主加载真实产物，驱动三种事件）：

```sh
cargo test
```

## 上传与配置

1. 在管理面板「插件」页上传 `plugin.tar.gz`（等价于
   `POST /api/plugins`，multipart 的 `plugin` 字段）。上传后默认不启用。
2. 点该行的「配置」，填 `plugin.toml` 里 `[[kv]]` 声明的两项（等价于
   `PUT /api/plugins/{id}/kv/{key}`，body `{"value": "..."}`）。对话框按声明
   显示标签、必填标记与提示，key 不用自己记：
   - `bot_token`：从 [@BotFather](https://t.me/BotFather) 拿到的 token
   - `chat_id`：目标会话 id（群为负数；可先给机器人发一条消息，再从
     `getUpdates` 的响应里找到 chat id）
3. 点「启用」，再用「测试」自检——它会构造一条合成的 `plugin_expiry_soon`
   事件，走与真实派发完全相同的执行路径。必填项没填时「测试」会直接告诉你
   缺哪一项。

   > 已安装过旧版的部署：库里的 manifest 是上传时抄的一份，`[[kv]]` 要
   > 生效得先删除旧版再上传（同名 `plugin_id` 重复上传会被拒）。

## 改造成自己的插件

`plugin_id` 是 kv 命名空间的一部分，上传前请换成自己的反向域（如
`io.github.<用户名>.my-notify`）；同名 `plugin_id` 已存在时上传会被拒绝，
需要先删除旧版。删除插件会连 kv 配置与 plugin_data 一起清掉。

`[[kv]]` 里的 key 必须与 `src/lib.rs` 中 `kv_get_string` 读的名字逐字
一致——两者不一致时，面板会要求你填一个插件永远读不到的字段。
