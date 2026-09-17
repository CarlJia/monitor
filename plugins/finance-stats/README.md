# finance-stats

monitor-hub 的财务统计插件（wasm 插件 ABI v2）。它自持全部节点的财务数据，
按 Frankfurter 汇率统一换算到自选币种，统计年化续费成本与剩余价值，展示
到期窗口内的机器列表，并发出到期提醒。

## 职责

| 导出 | 触发 | 做什么 |
|---|---|---|
| `on_tick` | 宿主每小时 | 导入（首次）、刷新汇率、扫描到期（滚动到期日 / 发 `plugin_expiry_soon`） |
| `render_page` | 面板打开页面 | 返回统计页的 JSON UI 描述 |
| `on_action` | 页面交互 | `set_currency` / `save_node` / `refresh_fx` |
| `on_cleanup` | 面板「清理」按钮 | 删掉已删除节点的残留记录 |

`on_event` 保留但为空——本插件不订阅宿主事件（它发出事件，由 tg-notify 之类订阅）。

## 数据模型（plugin_data）

| key | value |
|-----|-------|
| `config` | `{target_currency, threshold_days, imported}` |
| `fx` | `{base, rates:{CUR:number}, fetched_at}` |
| `node:<id>` | `{name, price, currency, billing_cycle, expires_at, purchased_at}` |

宿主 node 表自 ABI v2 起不再有价格/币种/周期/到期日列，这些字段的唯一真源
就是本插件的 `plugin_data`。

## 口径

- **年化续费成本** = `price ÷ 周期月数 × 12`，换算到目标币种后汇总。
- **剩余价值** = 周期机器按 `price × 剩余天数 ÷ 周期天数`；`once` 机器按购买日
  到到期日的总跨度折算。
- `billing_cycle = free`（页面上选「免费」）与 `price = 0` 的机器都不进两项
  汇总，到期列表里标记「免费」——标成免费的机器，价格列写多少都不作数。
- `billing_cycle = once` 不进年化成本（但仍按购买日折算剩余价值）。
- 首次导入建出的记录：价格 0、币种 USD、周期**年付**、无到期日。
- 到期列表窗口与 `plugin_expiry_soon` 阈值共用 `config.threshold_days`（默认 7 天）。

## 页面

- 汇总与币种切换同一行：第一格是「展示币种」下拉，后两格是年化成本与剩余
  价值（金额带币种符号，如 `¥128.40`）。汇率不可用时金额印「—」，下拉照常给。
- 编辑表的「价格」列右对齐、保留两位小数，前面缀同行币种的符号。币种符号只有
  插件里 `CURRENCIES` 这一份表：下拉文案（`¥ CNY`）、金额与价格前缀都由它派生。

## 汇率

- 源：`https://api.frankfurter.dev/v1/latest?base=<目标币种>`（GET，无需鉴权）。
- 拉取失败时沿用上一次缓存并标注汇率时间；从未成功则页面提示「汇率不可用」。

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

冒烟测试（用 wasmtime 桩宿主加载真实产物，驱动 tick / page / action / cleanup）：

```sh
cargo test
```

测试覆盖了年化换算、剩余价值折算、汇率降级、到期滚动与提醒、免费/一次性
机器的处理、切币种持久化与清理。

## 上传与启用

1. 管理面板「插件」页上传 `plugin.tar.gz`，然后启用。
2. 首次 tick（或首次打开页面）自动为每台机器建立财务记录并拉取汇率。
3. 打开「财务统计」页面即可查看与编辑。

> 升级提示：宿主的到期提醒已迁移到本插件。未启用本插件的部署不会有到期通知。
