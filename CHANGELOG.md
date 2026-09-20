# Changelog

All notable changes to monitor-hub will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.7] - 2026-09-20

### Added

- 财务统计页「汇总与币种选择」合成一行展示：年化续费总成本、剩余总价值与展示币种下拉同处一栏，操作员一眼能把两个数与按什么币种算的对上；汇率不可用时金额印「—」，下拉照常给（#12）。
- CI(plugin-contract)：拒绝契约 tag 时检查契约 crate 是否落后于主仓——上一次发布的 `monitor-plugin-contract-v2.0.0` 指向的 commit 落后 main 三个修复，ABI 已声明但 `ABI_VERSION` 未重导出、MockHttp 也没 `Arc`，拉 tag 编译插件会直接挂。比对 tag 与 main 的 crate 内容，任何差异即拒绝放行；祖先检查抓不到这种情况（stale commit 仍是 main 的祖先）。

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