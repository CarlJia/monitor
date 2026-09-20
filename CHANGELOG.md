# Changelog

All notable changes to monitor-hub will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.7] - 2026-09-20

### Added

- 财务统计页「汇总与币种选择」合成一行展示：年化续费总成本、剩余总价值与展示币种下拉同处一栏，操作员一眼能把两个数与按什么币种算的对上；汇率不可用时金额印「—」，下拉照常给（#12）。
- CI(plugin-contract)：拒绝契约 tag 时检查契约 crate 是否落后于主仓——上一次发布的 `monitor-plugin-contract-v2.0.0` 指向的 commit 落后 main 三个修复，ABI 已声明但 `ABI_VERSION` 未重导出、MockHttp 也没 `Arc`，拉 tag 编译插件会直接挂。比对 tag 与 main 的 crate 内容，任何差异即拒绝放行；祖先检查抓不到这种情况（stale commit 仍是 main 的祖先）。

## [2.0.6] - 2026-09-19

### Added

- 新增 `monitor-plugin-contract` crate：把 13 个宿主函数从 `src/plugin/host_funcs.rs` 抽到独立 crate（host 与 plugin 共用一份实现）。之前同样的代码在 hub 与插件测试里各有一份，宿主签名/返回码/预算变更容易漂移；现在 host 的 `PluginState` 实现 `Host + HasScratch` trait 直接复用，crate 也带 `ContractState`（内存后端 + 可插拔 http）供插件测试用。limits / error codes / `PLUGIN_EVENT_PREFIX` 常量随 crate 迁出作为单一来源，monitor 重新导出。根 `Cargo.toml` 加 `[workspace]`。

### Changed

- 插件页新增节点增删事件订阅 (`node_added` / `node_deleted`)；之前节点列表是只导入一次，新机器加进来后页面看不到，现在实时同步。
- CI(plugin-contract)：在 contract job 里 lint 契约 crate（与主仓同 lint 集），并跑契约 crate 的测试；同时把契约 crate 的版本同步作为发布门——契约 crate 版本落后主仓即拒绝 tag（见 [2.0.7] 的契约门条目）。
- 文档指向 fork 的仓库与镜像地址。

### Fixed

- web-admin 地址按地址族择优并分两行展示（IPv4 / IPv6 各一行），避免一行挤掉一族的可见性。
- hub 持久保留节点连接的观察地址：节点重连后仍能看到上次记录的入站地址，不再丢失上下文。
- plugin-contract：`data_list` 前缀与宿主实现一致；`MockHttp` 改 `Arc<MockHttp>` 让契约测试能多实例持有；fuel-aware gate；导出 `ABI_VERSION`；节点顺序与宿主 `nodes_query` 真实顺序对齐；版本同步收紧。
- admin：`cls-budget` 在 SPA navigation 后等 `DOMContentLoaded` 再读，避免 navigation 期间读到旧尺寸。

## [2.0.5] - 2026-09-19

### Fixed

- web-admin NAT 节点优先显示连接来源的公网地址：之前 NAT 后面节点只能看到内网地址，现在面板显示连接来源侧的公网地址（与节点 host 上的判断一致）。
- web-admin `isLocalV4` 与 agent `is_public` 同步排除 `TEST-NETs`：web-admin 之前的 `isLocalV4` 只排除 RFC1918，agent `is_public` 同时排除 IANA `TEST-NET-2/3` 与 240/4 reserved；现在两边判定一致，避免 NAT 节点被误判为「非内网」。

## [2.0.4] - 2026-09-18

### Added

- hub 从 `CF-Connecting-IP` 头部读取节点真实公网 IP：Cloudflare 反代后面节点的连接公网 IP 通过该头部传入，之前只能看到反代 IP，现在面板显示节点真实的入站公网 IP。
- agent 发布源指向正确的 fork：发布动作此前指向了非 fork 仓库，已修正。

### Changed

- 插件源码迁到 `monitor-hub-plugins` 子仓：所有 WASM 插件源（`tg-notify`、`finance-stats`）与脚手架 skill 移到独立仓，monitor 仓只保留宿主实现与 ABI 文档。
- CI(plugin-abi)：abi-gate 现在基于 `*/plugin.toml` 的 `abi_version` 字段派生构建/测试矩阵（之前是硬编码 `[tg-notify, finance-stats]`），新插件的整数门不会漏；anchor 文件不可读或 gh 失败 / transient error 时 fail closed，`fetch` 替换也加 `set -e` 防御。
- CI：gate host ABI 与 finance plugin_id 与发布版对齐——任何 PR 改了 host ABI / finance plugin_id 但没同步发布插件即失败。
- 文档：新增 `CHANGELOG.md`，记录 v1.0.0 至 v2.0.3 完整变更；README 末尾加更新日志速览链到本文件。

## [2.0.3] - 2026-09-18

### Added

- 插件上传支持同 `plugin_id` 更高版本就地替换：`plugin_data` 按 plugin_id 命名空间隔离，升级后历史记录保留；旧值由各插件的导入逻辑自行兜底（#13）。
- 后台上传插件按钮改为单按钮触发，与「安装主题」一致，不再先选文件再选操作（#11）。

### Changed

- CI：Rust 编译产物纳入缓存，消除重复冷编译（#10）。
- CI：插件测试拆为并行 job，`cargo check` 不再串行等两段编译（#10）。

## [2.0.2] - 2026-09-17

### Added

- 财务统计页：打开页面即拉一次汇率（无缓存时），成功后才进统计；失败原因与时间落到页面的提示条而不是淹没在日志里（#9）。
- 财务统计页：编辑表列头中文化，币种与计费周期改下拉，保存后返回成功提示（#9）。
- 后台：下拉选项支持显示 `{value, label}` 形态，首屏加加载态、补齐防御与测试（#9）。
- 后台：插件页面表单支持字段标签与下拉控件，`on_action` 响应可携带 `toast`（#9）。

### Changed

- 精简词汇表与财务插件实现：消除重复定义与多余的宿主调用（#9）。

### Fixed

- 删除插件不再请求已删插件的日志；空错误体不再弹空白 toast（#8）。
- 财务统计页：错误码文案测试覆盖到位，刷新失败提示与状态条共用一份文案（#9）。
- 后台插件页日志的并发残留与死兜底收敛（#8）。

## [2.0.1] - 2026-09-17

### Changed

- 后台「安全」页会话卡片改为与设置同帧渲染，消除 CLS 0.18（#5）。
- CLS 预算检查改读 Chrome 自报的调试地址，消除偶发假红；补齐门禁失败路径——假绿、挂死、进程泄漏都覆盖到（#5）。

### Fixed

- 插件数据面钩子改用独立 fuel 预算（`DEFAULT_HOOK_FUEL_LIMIT = 20,000,000`），多机器部署下页面不再 502（#6）。

## [1.2.1] - 2026-09-17

### Added

- 插件失败透出插件自己的日志：派发的 `detail` 现在带上插件经 `host_log` 写的话（不可见字符按规清洗），manifest 可声明必填配置（#4）。

### Changed

- 拆出 `host_funcs.rs` 与 `log.rs`：`host.rs` 由 1904 行降至 564 行，关注点单一（#4）。

## [1.2.0] - 2026-09-16

### Added

- **插件 ABI 升到 v2，宿主到期检测与财务四列退役**（`schedule: expiry-related, ts-side` + `feature/deadline: financial-fields`）：
  - 节点 JSON 去掉 `price` / `currency` / `billing_cycle` / `expires_at`，财务数据由插件自持。
  - v2 新增宿主函数：`data_put` / `data_get` / `data_delete` / `data_list`（插件自有 `plugin_data` 表）、`nodes_query`（节点基础信息只读）、`emit_event`（事件名强制 `plugin_` 前缀）、`http_get`。
  - 宿主自身只发 `agent_offline` / `agent_online` 两个事件；到期提醒改由财务插件经 `emit_event` 发 `plugin_expiry_soon`。
- **http 宿主函数加 SSRF 防线**：拒绝私有 / 保留 / 回环 / 链路本地 / CGNAT / 云元数据段（错误码 -9）；仅作用于插件，宿主自身的 GitHub / 主题 / `github_proxy` 镜像不受此限（PR #2 review fixes）。
- **批量网络质量接口** `GET /api/nodes/quality`：一次拉全部节点的延迟 / 丢包 / 抖动。
- **schema v6**：删除 node 表退役的财务四列。两段式门控——只有 `plugin_data` 出现 `node:` 记录（财务插件已导入）才删，否则停在 v5 下次重试；旧值不迁移，由插件导入逻辑补建。
- **finance-stats** 财务统计插件：ABI v2 首个数据型插件（tick + page + cleanup + emit_event + data_*）。
- **tg-notify** 迁移到插件 ABI v2：仍订阅 `agent_offline` / `agent_online`，新增订阅财务插件的 `plugin_expiry_soon`。
- 插件页面协议端点：`/api/plugins/<id>/page`、`/api/plugins/<id>/action`；清理端点 `on_cleanup`；插件空间占用统计端点。
- 插件列表带出 v2 能力声明（page / tick / cleanup）。
- `HISTORY_GATE` 由进程级静态挪到 `App.history_gate`（per-instance），并行测试天然隔离，全量 15 轮零失败。
- IPv6 方括号字面量识别：SSRF 判定认得出 `[::1]` 形 URL。

### Fixed

- 事件幂等键混合（通知去重不再因字段顺序不同而漏命中）。
- 缺汇率币种在面板给出明确提示，不让汇总数字看起来「只是 0」。

### Chore

- CI 过 fmt / clippy；前端表单重挂载去掉 effect 清状态。

## [1.1.0] - 2026-09-15

### Added

- Zima 风格展示优化（#1：节点卡片、状态指示、布局密度）。

## [1.0.0] - 2026-09-15

### Added

- 初始版本。
- hub（axum + SQLite）+ agent（Linux）+ 内置默认主题三仓布局，WebSocket / JSON-RPC 2.0 通信。
- 后台 + 公开状态页。
- 续费货币单位新增 CAD 支持。
- web-theme.pin 指向 theme v1.0.0。
- `scripts/dev.sh`：本地同时启动 hub 与 Vite 面板。

### Fixed

- 经 CDN 代理的节点 `country` 一直为空。
- web-admin：按钮悬浮不切换手势光标。

### Chore

- 安装脚本仓库地址改为 `CarlJia/monitor`。
- `.gitignore` 新增 `.codegraph` 与 `.idea`。

## 历史发布 Tag / 版本错位（只读，不重建）

下列 tag 在发布时**没有同步更新** `Cargo.toml` 里的版本号（或指向了错的 commit）。重建需要 `push --force`，会影响远端已引用的 tag；本节只标记、保留 tag 不动。

如需按 commit 内 `Cargo.toml` 反查 tag，使用末尾的「重建映射」。

### 类型 A：发布漏改（hub `Cargo.toml` 没跟着 tag 升级）

| Tag | 指向 commit 的 `version` | 期望 | 状态 |
|-----|--------------------------|------|------|
| `v1.1.0` | `1.0.0` | `1.1.0` | 项目第一次发版，hub Cargo.toml 保留 1.0.0 |
| `v1.2.0` | `2.0.0` | `1.2.0` | v1.x → v2.x 升级期，Cargo.toml 已先行跨到 2.0.0 |
| `v1.2.1` | `2.0.0` | `1.2.1` | 同上 |
| `v2.0.1` | `2.0.1` | `2.0.1` | ✓ |
| `v2.0.2` | `2.0.1` | `2.0.2` | 发布漏改 |
| `v2.0.3` | `2.0.1` | `2.0.3` | 发布漏改 |
| `v2.0.4` | `2.0.1` | `2.0.4` | 发布漏改 |
| `v2.0.5` | `2.0.1` | `2.0.5` | 发布漏改 |
| `v2.0.6` | `2.0.1` | `2.0.6` | 发布漏改 |

### 类型 B：tag 指向错位的 commit（已本地重建）

| Tag | 旧指向 | 新指向 | 说明 |
|-----|--------|--------|------|
| `monitor-plugin-contract-v2.0.0` | `a868285` (v2.0.6 merge commit) | `5490db0`（contract crate 首次引入 commit） | 旧指向是 v2.0.6 的 merge commit，但 contract crate 在 main 上落后三个修复——这是 [2.0.7] 那条 CI 契约门拦截的场景。本地已重打到 `5490db0`（contract crate 首次引入 commit，contract = 2.0.0）。远端未推。 |

### 重建映射（按 commit 内 `Cargo.toml` 反查 tag）

- `Cargo.toml = "1.0.0"` → tag `v1.0.0` ✓（同时也是 `v1.1.0` / `v1.2.0` / `v1.2.1` 三个 tag 指向的 commit——类型 A）
- `Cargo.toml = "2.0.1"` → tag `v2.0.1` ✓（同时也是 `v2.0.2` ~ `v2.0.6` 五个 tag 指向的 commit——类型 A）
- `Cargo.toml = "2.0.7"` → tag `v2.0.7` ✓（[2.0.7] 段）
- `monitor-plugin-contract = "2.0.7"` → commit `8f95116`（[2.0.7] 段），contract 子 crate 跟 hub 同步升
- `monitor-plugin-contract = "2.0.0"` → commit `5490db0`，对应本地重建后的 `monitor-plugin-contract-v2.0.0`

---

## 版本规约提醒

- **插件 ABI v2 是唯一版本**：v1 插件不再加载，升级前需对着 v2 重编并重新上传（README「升级到 2.0.0 的破坏性变更」）。
- **插件事件以 `plugin_` 前缀命名**，宿主自身只剩 `agent_offline` / `agent_online`。
- **到期通知**：未安装 `finance-stats` 的部署不再有到期通知（预期行为）；财务字段已从宿主 node 表移除。
- **`http_get` / `http_post` 插件目标**：必须是 https 且不得落在私有 / 保留网段（-9）；仅插件受此限，宿主自身调用不受影响。
- **数据归属**：插件对宿主 node 表只读，业务数据由插件经 `plugin_data` 自持。

[Unreleased]: https://github.com/CarlJia/monitor/compare/v2.0.3...HEAD
[2.0.3]: https://github.com/CarlJia/monitor/compare/v2.0.2...v2.0.3
[2.0.2]: https://github.com/CarlJia/monitor/compare/v2.0.1...v2.0.2
[2.0.1]: https://github.com/CarlJia/monitor/compare/v1.2.1...v2.0.1
[1.2.1]: https://github.com/CarlJia/monitor/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/CarlJia/monitor/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/CarlJia/monitor/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/CarlJia/monitor/releases/tag/v1.0.0