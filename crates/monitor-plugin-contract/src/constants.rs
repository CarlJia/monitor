//! 宿主侧的限额与形态常数。**唯一一份定义**:monitor 主 crate 的 `plugin::host`
//! 反过来 `pub use` 这里——改限额只可能改一处,契约与生产不会静默分叉。

use std::time::Duration;

/// 本 crate 实现的宿主 ABI 版本。宿主 `src/plugin/manifest.rs` 的 `ABI_VERSION`
/// 必须等于它(monitor `ci.yml` 的 `contract` job 断言),插件仓的契约门也断言
/// 「拉到的契约 ABI_VERSION == 本插件 `plugin.toml` 声明的 `abi_version`」——
/// 这样「宿主面变了但没 bump / 没打新 tag」导致拉到旧契约时,门会红而不是验错对象。
pub const ABI_VERSION: i64 = 2;

/// 每次调用的默认 fuel 限额(KTD6)。dispatch 每次读 setting
/// `plugin.fuel_limit` 覆写,缺省回落到这里。
pub const DEFAULT_FUEL_LIMIT: u64 = 1_000_000;

/// ABI v2 数据面钩子(`on_tick`、`render_page`、`on_action`、`on_cleanup`)的
/// 默认 fuel 限额,比事件派发那档宽 20 倍。
///
/// 分成两档是因为两者的工作量量级不同:派发收到的是**有界**的事件载荷(节点
/// id/名字),1,000,000 足够;而这些钩子读的是插件自己的 plugin_data,开销随
/// 数据规模线性增长——财务插件渲染页面要读它全部节点的财务记录,实测空页面
/// 44 万、每台机器再 5.4 万(每小时 tick 是 67 万 + 每台 2.4 万),十来台机器就
/// 把派发那档预算烧穿:页面端点回 502,tick 静默停在 `fuel_exhausted`。
/// 20,000,000 对当前的财务插件够约 365 台;插件的开销结构变了(或机器更多)时
/// 由 setting `plugin.hook_fuel_limit` 覆写。
pub const DEFAULT_HOOK_FUEL_LIMIT: u64 = 20_000_000;

/// 单个插件的 kv 值上限:8 KiB,足够放渠道配置,又不会让 setting 表被一个插件
/// 当对象存储用。面板的 kv 写入引用同一个上限:「面板能写的不能比插件运行时
/// 能写的多」,否则面板成了绕过插件存储限额的后门。
pub const KV_VALUE_MAX: usize = 8 * 1024;

/// kv 的 key 上限,128 字节。面板、manifest 的 `[[kv]]` 声明与 kv 命名空间
/// 共用这一个数字:三处各写一份,迟早会漂。
pub const KV_KEY_MAX: usize = 128;

/// 插件 http 请求的墙钟超时(A13):4 秒,落在 5 秒的派发预算内,留 1 秒给宿主
/// 自己的开销。挂在请求上:插件走 `App::plugin_http`(不跟随重定向的那个),
/// 它的 client 级超时是 15 秒,服务于宿主侧下载,不能为插件收短。
pub const HTTP_TIMEOUT: Duration = Duration::from_secs(4);

/// 单次 http 响应体的硬上限:64 KiB。插件声明的 resp_cap 再大也读这么多——
/// 有界下载要防的正是"cap 被声明成超大值/响应体本身无限大"的内存放大。
pub const HTTP_RESP_MAX: usize = 64 * 1024;

/// 单条 plugin_data 记录的上限:256 KiB(KTD3)。财务记录是百台机器量的
/// JSON,远低于此;上限防的是插件把它当大对象存储用。
pub const RECORD_MAX: usize = 256 * 1024;

/// 单插件 plugin_data 的总配额:16 MiB(KTD3),与上传包上限同量级的防御值。
pub const PLUGIN_DATA_MAX: i64 = 16 * 1024 * 1024;

/// `emit_event` 只接受这个前缀的事件名——插件发的事件走总线,必须一眼与宿主
/// 事件分得开。manifest 侧的事件名校验引用同一个常数。
pub const PLUGIN_EVENT_PREFIX: &str = "plugin_";
