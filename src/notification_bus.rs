//! The notification bus: the event vocabulary every source emits and the
//! deduplication that decides whether an emission becomes a dispatch.
//!
//! Event sources never talk to plugins. They call [`emit`], which asks the
//! database whether this particular alert has already gone out and, if not,
//! hands the event to the plugin registry. The rules live here rather than in
//! each source, so connectivity checking and expiry scanning cannot drift
//! apart in what they suppress.

use anyhow::Result;
use chrono::{Datelike, NaiveDate, Utc};

use crate::App;

/// 登录事件按内容 + 时刻算派发键的字段分隔符。用一个正常不会出现在 method /
/// actor / reason / ip 里的字节,免得 `("a","b")` 与 `("ab","")` 撞成同一个键。
const LOGIN_KEY_SEP: char = '\u{1f}';

/// The event types the bus carries. Field set is what a plugin needs to
/// render a notification a human can act on.
///
/// Serialized with a `type` tag, so the payload handed to a plugin's
/// `on_event` reads `{"type":"agent_offline","node_id":1,...}` -- a shape
/// stable across plugin versions, since a WASM guest deserializes by field
/// name and tolerates additions.
///
/// v2: plugins emit their own events through the `emit_event` host function
/// as [`Event::Plugin`]; the name must start with `plugin_` (enforced host-side,
/// KTD6). `plugin_expiry_soon` replaces the retired host-side `expiry_soon`
/// and carries the same payload fields (`node_id`, `name`, `expires_at`,
/// `days_left`, `threshold_days`).
///
/// v2 also carries the node lifecycle: `NodeAdded` when a node row is created
/// (panel or automatic registration) and `NodeDeleted` when one is removed.
/// Both carry the node's own `created_at`, because a subscriber has to tell one
/// *machine* from another -- SQLite hands a deleted node's id to the next node
/// created -- and the two can reach a subscriber out of order.
///
/// 面板登录事件 `LoginSucceeded` / `LoginFailed` 同样是宿主事件:密码登录与
/// GitHub 登录各有成功/失败两路,`method` 区分渠道(`password` / `github`),
/// `actor` 是登录主体(GitHub 用户名;应急密码没有账号,留空),失败另带
/// `reason`,两者都带发起端 `ip` 与 `observed_at`。它们不绑节点(`node_id` 恒
/// 为 0),也不是状态事件——每次尝试都是独立一条,去重键按内容 + 时刻算(见
/// [`Event::threshold_or_state_key`]),所以同一秒内两次不同的尝试各派发一条,
/// 而重复回放的同一条仍被抑制。
#[derive(Debug, Clone)]
pub enum Event {
    AgentOffline { node_id: i64, name: String, observed_at: i64, last_seen_at: i64 },
    AgentOnline { node_id: i64, name: String, observed_at: i64 },
    NodeAdded { node_id: i64, name: String, created_at: i64 },
    NodeDeleted { node_id: i64, name: String, created_at: i64 },
    LoginSucceeded { method: String, actor: String, ip: String, observed_at: i64 },
    LoginFailed { method: String, reason: String, ip: String, observed_at: i64 },
    Plugin { name: String, payload: serde_json::Value },
}

/// 手工实现 Serialize(而非 derive 的内部标签枚举):宿主事件的 `type` 是
/// 变体名,插件事件的 `type` 是插件自报的事件名——内部标签枚举的 tag 值
/// 无法由数据驱动,所以 `Plugin` 变体在这里把 `name` 摊到 `type`、payload
/// 摊到顶层,插件收到的形状与宿主事件一致:
/// `{"type":"plugin_expiry_soon","node_id":7,...}`。
impl serde::Serialize for Event {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        match self {
            Event::AgentOffline { node_id, name, observed_at, last_seen_at } => {
                let mut m = s.serialize_map(Some(5))?;
                m.serialize_entry("type", Self::AGENT_OFFLINE)?;
                m.serialize_entry("node_id", node_id)?;
                m.serialize_entry("name", name)?;
                m.serialize_entry("observed_at", observed_at)?;
                m.serialize_entry("last_seen_at", last_seen_at)?;
                m.end()
            }
            Event::AgentOnline { node_id, name, observed_at } => {
                let mut m = s.serialize_map(Some(4))?;
                m.serialize_entry("type", Self::AGENT_ONLINE)?;
                m.serialize_entry("node_id", node_id)?;
                m.serialize_entry("name", name)?;
                m.serialize_entry("observed_at", observed_at)?;
                m.end()
            }
            Event::NodeAdded { node_id, name, created_at }
            | Event::NodeDeleted { node_id, name, created_at } => {
                let mut m = s.serialize_map(Some(4))?;
                m.serialize_entry("type", self.type_name())?;
                m.serialize_entry("node_id", node_id)?;
                m.serialize_entry("name", name)?;
                m.serialize_entry("created_at", created_at)?;
                m.end()
            }
            Event::LoginSucceeded { method, actor, ip, observed_at } => {
                let mut m = s.serialize_map(Some(5))?;
                m.serialize_entry("type", Self::LOGIN_SUCCEEDED)?;
                m.serialize_entry("method", method)?;
                m.serialize_entry("actor", actor)?;
                m.serialize_entry("ip", ip)?;
                m.serialize_entry("observed_at", observed_at)?;
                m.end()
            }
            Event::LoginFailed { method, reason, ip, observed_at } => {
                let mut m = s.serialize_map(Some(5))?;
                m.serialize_entry("type", Self::LOGIN_FAILED)?;
                m.serialize_entry("method", method)?;
                m.serialize_entry("reason", reason)?;
                m.serialize_entry("ip", ip)?;
                m.serialize_entry("observed_at", observed_at)?;
                m.end()
            }
            Event::Plugin { name, payload } => {
                let mut m = s.serialize_map(None)?;
                m.serialize_entry("type", name)?;
                if let serde_json::Value::Object(obj) = payload {
                    for (k, v) in obj {
                        // payload 里若带 `type`,事件名优先——插件不能靠它改写路由词。
                        if k != "type" {
                            m.serialize_entry(k, v)?;
                        }
                    }
                }
                m.end()
            }
        }
    }
}

impl Event {
    /// 事件词表的单一来源:db 的状态行比较、manifest 校验与扫描循环都引用
    /// 这组名字,散写字面量会让两处悄悄漂移。插件事件(`plugin_` 前缀)不在
    /// 词表内——它们由各插件运行时发出,宿主无法预知全集。
    pub const AGENT_OFFLINE: &'static str = "agent_offline";
    pub const AGENT_ONLINE: &'static str = "agent_online";
    pub const NODE_ADDED: &'static str = "node_added";
    pub const NODE_DELETED: &'static str = "node_deleted";
    pub const LOGIN_SUCCEEDED: &'static str = "login_succeeded";
    pub const LOGIN_FAILED: &'static str = "login_failed";
    /// v2 支持的全部宿主自身事件名,manifest 的 `subscribes` 逐项对照。
    pub const KNOWN: [&'static str; 6] = [
        Self::AGENT_OFFLINE,
        Self::AGENT_ONLINE,
        Self::NODE_ADDED,
        Self::NODE_DELETED,
        Self::LOGIN_SUCCEEDED,
        Self::LOGIN_FAILED,
    ];

    /// The discriminator stored in `notification_log.event_type` and carried in
    /// the JSON `type` tag. A lifetime `&'static str` rather than a String:
    /// it is compared against database rows on every emission.
    pub fn type_name(&self) -> &str {
        match self {
            Event::AgentOffline { .. } => Self::AGENT_OFFLINE,
            Event::AgentOnline { .. } => Self::AGENT_ONLINE,
            Event::NodeAdded { .. } => Self::NODE_ADDED,
            Event::NodeDeleted { .. } => Self::NODE_DELETED,
            Event::LoginSucceeded { .. } => Self::LOGIN_SUCCEEDED,
            Event::LoginFailed { .. } => Self::LOGIN_FAILED,
            Event::Plugin { name, .. } => name,
        }
    }

    pub fn node_id(&self) -> i64 {
        match self {
            Event::AgentOffline { node_id, .. }
            | Event::AgentOnline { node_id, .. }
            | Event::NodeAdded { node_id, .. }
            | Event::NodeDeleted { node_id, .. } => *node_id,
            // 登录事件不绑节点:去重键的 node_id 一律 0(键的区分靠内容 + 时刻)。
            Event::LoginSucceeded { .. } | Event::LoginFailed { .. } => 0,
            Event::Plugin { payload, .. } => payload.get("node_id").and_then(|v| v.as_i64()).unwrap_or(0),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Event::AgentOffline { name, .. }
            | Event::AgentOnline { name, .. }
            | Event::NodeAdded { name, .. }
            | Event::NodeDeleted { name, .. } => name,
            // 登录主体即「名字」:成功事件是 actor(GitHub 用户名,应急密码为空),
            // 失败事件没有确定主体,留空。
            Event::LoginSucceeded { actor, .. } => actor,
            Event::LoginFailed { .. } => "",
            Event::Plugin { payload, .. } => payload.get("name").and_then(|v| v.as_str()).unwrap_or_default(),
        }
    }

    /// The idempotency key stored in `notification_log.threshold_or_state_key`.
    ///
    /// State events return 0: they hold one mutable row per node, and the
    /// row's identity is the node alone. `ExpirySoon` used to encode
    /// `threshold_days * 1_000_000 + expires_at 的自公历纪元起的天数`
    /// (`NaiveDate::num_days_from_ce`): the tier keeps the thresholds from
    /// colliding with one another, and the expiry date makes the key specific
    /// to this billing cycle, so a renewal that rolls the date forward lets
    /// the same tier fire again. `num_days_from_ce` rather than a Unix day so
    /// the encoding needs no timezone choice -- it only has to differ per
    /// date, never be a wall-clock figure. An unparseable date contributes 0,
    /// which still keeps tiers distinct rather than collapsing them.
    ///
    /// v2: [`Event::Plugin`] reuses the same encoding for expiry-shaped
    /// payloads (`threshold_days` + `expires_at`, both read from the payload).
    /// A payload without those fields mixes in the event name's hash so two
    /// plugin events with different names don't collide on the same row
    /// (both fallback values would otherwise be 0 and the dedup would suppress
    /// them as the same alert).
    pub fn threshold_or_state_key(&self) -> i64 {
        match self {
            Event::AgentOffline { .. } | Event::AgentOnline { .. } => 0,
            // 身份取节点自己的创建时间戳:同一台机器的同一次创建只派发一次,
            // 而 id 被复用后新建的那台是另一个值,不会被旧行抑制掉。
            Event::NodeAdded { created_at, .. } | Event::NodeDeleted { created_at, .. } => *created_at,
            // 每次登录尝试是独立一条,键必须逐次不同——否则同一 (node_id=0,
            // event_type) 的第二次就被永久去重掉,失败告警只发第一条。键按
            // 「时刻 + 内容」的 FNV-1a 算:同一秒里两次不同的尝试(ip/主体/原因
            // 有别)得到不同的键、各派发一条;而重复回放的同一条(测试或重试同一
            // 事件)算出同一个键,仍被 record_dispatch 的 INSERT OR IGNORE 抑制。
            // FNV-1a 而不是 DefaultHasher:后者算法未指定、跨 Rust 版本会变,而这
            // 个值要持久化进 notification_log,变了就会重发。
            Event::LoginSucceeded { method, actor, ip, observed_at } => {
                let content = format!(
                    "{observed_at}{LOGIN_KEY_SEP}{method}{LOGIN_KEY_SEP}{actor}{LOGIN_KEY_SEP}{ip}"
                );
                fnv1a(content.as_bytes()) as i64
            }
            Event::LoginFailed { method, reason, ip, observed_at } => {
                let content = format!(
                    "{observed_at}{LOGIN_KEY_SEP}{method}{LOGIN_KEY_SEP}{reason}{LOGIN_KEY_SEP}{ip}"
                );
                fnv1a(content.as_bytes()) as i64
            }
            Event::Plugin { payload, .. } => {
                let threshold = payload.get("threshold_days").and_then(|v| v.as_i64()).unwrap_or(0);
                let day = payload
                    .get("expires_at")
                    .and_then(|v| v.as_str())
                    .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
                    .map(|d| d.num_days_from_ce() as i64)
                    .unwrap_or(0);
                let base = threshold * 1_000_000 + day;
                if base == 0 {
                    // 没有 `threshold_days`、也没有 `expires_at` 的插件事件:键会
                    // 退化成一个常量,同一 (节点, 事件名) 的第二次发射被永久去重
                    // ——但两次的 payload 可能完全不同。用 payload 的内容哈希补
                    // 足身份:内容相同的重复仍按同一条处理,内容不同的各自派发。
                    //
                    // FNV-1a 而不是 `DefaultHasher`:后者算法未指定,跨 Rust 版本
                    // 会变,而这个值要持久化进 notification_log,变了就会重发。
                    fnv1a(payload.to_string().as_bytes()) as i64
                } else {
                    base
                }
            }
        }
    }

    /// True for the two sides of connectivity. State events are deduplicated
    /// by transition rather than by a content key: an offline node flapping
    /// its connection is one alert, not a stream of them.
    ///
    /// The node lifecycle events are *not* state events: `transition_state_event`
    /// only knows the two connectivity sides, and every node event would look
    /// like a transition into a side of its own — one row per emission, with no
    /// deduplication to show for it. They carry their own key instead.
    pub fn is_state_event(&self) -> bool {
        matches!(self, Event::AgentOffline { .. } | Event::AgentOnline { .. })
    }
}

/// FNV-1a(64 位)。用于给没有 `threshold_days`/`expires_at` 的插件事件算一个
/// 稳定的内容键——`DefaultHasher` 的算法未指定,跨 Rust 版本会变,而这个值要
/// 持久化进 `notification_log`,变了会把已抑制的告警重新发一遍。
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Emits an event: deduplicate, record, then hand to the plugin registry.
///
/// A duplicate is not an error -- the source keeps scanning either way -- so
/// every skip is `Ok(())`. The `Result` carries only database failures; the
/// dispatch itself is fire-and-forget, and its outcome is written back by the
/// runtime (U4) via `mark_dispatch_result` rather than awaited here.
pub fn emit(app: &App, event: &Event) -> Result<()> {
    if event.is_state_event() {
        // One row per node holds whichever side the node is on; a repeat of
        // the same side is the same outage window and must not re-alert. The
        // transition is itself the check: it reads the standing row first and
        // returns false -- writing nothing -- when the node is already on this
        // side, and it clears the opposite side's row in the same transaction
        // when it does write.
        if !app.db.transition_state_event(event.node_id(), event.type_name(), Utc::now().timestamp())? {
            return Ok(()); // Same side already stands, or lost a race with a
                           // concurrent emission of it.
        }
    } else {
        // ExpirySoon: one alert per node, per tier, per billing cycle, keyed
        // by `threshold_or_state_key`.
        let key = event.threshold_or_state_key();
        if app.db.dispatch_already_sent(event.node_id(), event.type_name(), key)? {
            return Ok(());
        }
        if !app.db.record_dispatch(event.node_id(), event.type_name(), key, Utc::now().timestamp())? {
            return Ok(()); // Lost a race: the standing row is the other emission's.
        }
    }
    app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch(event);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;

    fn app() -> App {
        App::for_test(Db::open(":memory:").unwrap())
    }

    fn expiry(node_id: i64, expires_at: &str, days_left: i64, threshold_days: i64) -> Event {
        Event::Plugin {
            name: "plugin_expiry_soon".into(),
            payload: serde_json::json!({
                "node_id": node_id,
                "name": "edge-1",
                "expires_at": expires_at,
                "days_left": days_left,
                "threshold_days": threshold_days,
            }),
        }
    }

    /// Every assertion goes through the public db surface U1 shipped: the bus's
    /// contract is that a suppressed emission leaves no row and no dispatch,
    /// and the dispatch count on the stub registry confirms the bus forwarded
    /// the rest.
    fn recorded(app: &App, node_id: i64, event_type: &str, key: i64) -> bool {
        app.db.dispatch_already_sent(node_id, event_type, key).unwrap()
    }

    #[test]
    fn an_expiry_event_emits_once_per_key() {
        let app = app();
        let event = expiry(7, "2026-10-01", 7, 7);
        let key = event.threshold_or_state_key();
        emit(&app, &event).unwrap();
        emit(&app, &event).unwrap();
        assert!(recorded(&app, 7, "plugin_expiry_soon", key), "the first emission records");
        assert_eq!(app.plugins.read().unwrap().dispatch_count(), 1, "the duplicate must not re-dispatch");
    }

    #[test]
    fn a_renewed_cycle_realerts_at_the_same_tier() {
        let app = app();
        let old = expiry(7, "2026-10-01", 7, 7);
        emit(&app, &old).unwrap();
        // Same tier, same date: suppressed.
        emit(&app, &expiry(7, "2026-10-01", 6, 7)).unwrap();
        assert_eq!(app.plugins.read().unwrap().dispatch_count(), 1);
        // The date rolled forward by a renewal: a new billing cycle's key.
        let renewed = expiry(7, "2026-11-01", 30, 7);
        emit(&app, &renewed).unwrap();
        assert!(recorded(&app, 7, "plugin_expiry_soon", renewed.threshold_or_state_key()));
        assert_eq!(app.plugins.read().unwrap().dispatch_count(), 2);
    }

    #[test]
    fn state_events_transition_rather_than_accumulate() {
        let app = app();
        let offline =
            Event::AgentOffline { node_id: 5, name: "edge-1".into(), observed_at: 100, last_seen_at: 90 };
        let online = Event::AgentOnline { node_id: 5, name: "edge-1".into(), observed_at: 300 };

        emit(&app, &offline).unwrap();
        assert_eq!(app.db.current_state_event(5).unwrap().map(|(t, _)| t), Some("agent_offline".into()));

        // Same side again within the same window: no re-dispatch.
        emit(&app, &offline).unwrap();
        assert_eq!(app.plugins.read().unwrap().dispatch_count(), 1);

        // Coming back clears the offline row rather than joining it: the key
        // both sides share would otherwise report the outage forever.
        emit(&app, &online).unwrap();
        assert_eq!(app.db.current_state_event(5).unwrap().map(|(t, _)| t), Some("agent_online".into()));
        assert!(!recorded(&app, 5, "agent_offline", 0), "the opposite side's row must be cleared");

        // Going offline again is a new outage and must re-alert.
        emit(&app, &offline).unwrap();
        assert_eq!(app.plugins.read().unwrap().dispatch_count(), 3);
        assert!(recorded(&app, 5, "agent_offline", 0));
    }

    /// The key is the ExpirySoon idempotency contract in one number: tiers
    /// must not collide with each other, dates must not collide with each
    /// other, and state events are always the shared 0.
    #[test]
    fn the_key_encodes_tier_and_cycle() {
        let k = |expires_at: &str, threshold_days: i64| {
            expiry(1, expires_at, 7, threshold_days).threshold_or_state_key()
        };
        assert_ne!(k("2026-10-01", 7), k("2026-10-01", 3), "different tiers");
        assert_ne!(k("2026-10-01", 7), k("2026-11-01", 7), "different cycles");
        assert_eq!(k("2026-10-01", 7), k("2026-10-01", 7));
        // 30 days is also the days-in-a-month case: 1_000_000 outclasses any
        // day count, so no tier's dates bleed into the next tier's.
        assert_eq!(k("2026-12-31", 1) / 1_000_000, 1);
        // State events are keyed by the node alone.
        let offline =
            Event::AgentOffline { node_id: 1, name: String::new(), observed_at: 0, last_seen_at: 0 };
        let online = Event::AgentOnline { node_id: 1, name: String::new(), observed_at: 0 };
        assert_eq!(offline.threshold_or_state_key(), 0);
        assert_eq!(online.threshold_or_state_key(), 0);
        assert!(offline.is_state_event() && online.is_state_event());
        assert!(!expiry(1, "2026-10-01", 7, 7).is_state_event());

        // 节点事件按创建时间戳定身份,不是状态事件。
        let added = Event::NodeAdded { node_id: 1, name: String::new(), created_at: 4_242 };
        let deleted = Event::NodeDeleted { node_id: 1, name: String::new(), created_at: 4_242 };
        assert_eq!(added.threshold_or_state_key(), 4_242);
        assert_eq!(deleted.threshold_or_state_key(), 4_242);
        assert!(!added.is_state_event() && !deleted.is_state_event());
    }

    /// 节点事件按创建时间戳去重:同一台机器只派发一次,而 id 被复用后新建的
    /// 那台是另一个键——订阅者不会漏掉「这个 id 换了机器」这件事。
    #[test]
    fn node_events_are_keyed_by_creation_not_by_id() {
        let app = app();
        let first = Event::NodeAdded { node_id: 5, name: "edge-1".into(), created_at: 100 };
        emit(&app, &first).unwrap();
        emit(&app, &first).unwrap();
        assert_eq!(app.plugins.read().unwrap().dispatch_count(), 1, "同一次创建只派发一次");

        // id 被复用:同一个 id、新的创建时间,是另一台机器,必须派发。
        let second = Event::NodeAdded { node_id: 5, name: "edge-2".into(), created_at: 900 };
        emit(&app, &second).unwrap();
        assert_eq!(app.plugins.read().unwrap().dispatch_count(), 2, "复用 id 的新机器不能被旧行吞掉");
    }

    /// 节点生命周期事件不碰连通性状态:那对「在线/离线」行按节点只存一侧,
    /// 节点事件挤进去会让离线扫描读到错误的当前状态。
    #[test]
    fn node_events_do_not_touch_connectivity_state() {
        let app = app();
        emit(&app, &Event::NodeAdded { node_id: 5, name: "edge-1".into(), created_at: 100 }).unwrap();
        emit(&app, &Event::NodeDeleted { node_id: 5, name: "edge-1".into(), created_at: 100 }).unwrap();
        assert_eq!(app.db.current_state_event(5).unwrap(), None, "节点事件不写连通性状态行");
        assert_eq!(app.plugins.read().unwrap().dispatch_count(), 2, "两个事件照常派发");
    }

    /// 每个宿主事件的 `type_name` 都必须在 `KNOWN` 词表里:manifest 的 `subscribes`
    /// 校验、db 的状态行与扫描循环共用这一份词表,漏掉一个就等于那个事件谁也没法
    /// 订阅——而代码仍然编译通过。
    #[test]
    fn every_host_variant_is_in_the_known_vocabulary() {
        let all = [
            Event::AgentOffline { node_id: 1, name: String::new(), observed_at: 0, last_seen_at: 0 },
            Event::AgentOnline { node_id: 1, name: String::new(), observed_at: 0 },
            Event::NodeAdded { node_id: 1, name: String::new(), created_at: 0 },
            Event::NodeDeleted { node_id: 1, name: String::new(), created_at: 0 },
            Event::LoginSucceeded {
                method: "password".into(),
                actor: String::new(),
                ip: "1.2.3.4".into(),
                observed_at: 0,
            },
            Event::LoginFailed {
                method: "github".into(),
                reason: "bad".into(),
                ip: "1.2.3.4".into(),
                observed_at: 0,
            },
        ];
        for event in &all {
            assert!(Event::KNOWN.contains(&event.type_name()), "{} 不在 KNOWN 词表里", event.type_name());
        }
    }

    /// 登录事件每次尝试独立派发:同一秒里内容不同的两次尝试(ip/主体/原因有别)
    /// 算出不同的键,各留一条;而回放的同一条算出同一个键,被去重抑制。这条守卫
    /// 把登录事件与 node 事件(键取 created_at,同一次创建净效果为零)区分开——
    /// 前者一旦退化成常量键,失败告警就只会发第一条。
    #[test]
    fn login_events_key_by_content_so_each_attempt_dispatches() {
        let now = 1_700_000_000;
        let a = Event::LoginFailed {
            method: "password".into(),
            reason: "invalid password".into(),
            ip: "10.0.0.1".into(),
            observed_at: now,
        };
        // 同一秒、同一渠道,但来自另一个地址:必须是另一条。
        let b = Event::LoginFailed {
            method: "password".into(),
            reason: "invalid password".into(),
            ip: "10.0.0.2".into(),
            observed_at: now,
        };
        // 与 a 逐字段相同:回放的同一条,键必须一致(才会被去重抑制)。
        let a_again = Event::LoginFailed {
            method: "password".into(),
            reason: "invalid password".into(),
            ip: "10.0.0.1".into(),
            observed_at: now,
        };
        assert_ne!(
            a.threshold_or_state_key(),
            b.threshold_or_state_key(),
            "不同地址的两次失败尝试不该撞成同一个键"
        );
        assert_eq!(
            a.threshold_or_state_key(),
            a_again.threshold_or_state_key(),
            "同一条事件回放必须得到同一个键"
        );
        // 成功与失败即使内容凑巧一致,event_type 也把它们分到不同的 (node_id,
        // event_type, key) 主键上,这里再确认两类各自的键算得出来、非零。
        let ok = Event::LoginSucceeded {
            method: "github".into(),
            actor: "carl".into(),
            ip: "10.0.0.1".into(),
            observed_at: now,
        };
        assert_ne!(ok.threshold_or_state_key(), 0);
        assert!(!ok.is_state_event(), "登录事件不是状态事件,应走内容键去重");
    }

    /// The payload handed to a plugin, for U3/U8's ABI. Any change to this
    /// shape is a breaking plugin change.
    #[test]
    fn the_json_payload_carries_a_type_tag() {
        let event = Event::Plugin {
            name: "plugin_expiry_soon".into(),
            payload: serde_json::json!({
                "node_id": 7,
                "name": "edge-1",
                "expires_at": "2026-10-01",
                "days_left": 7,
                "threshold_days": 7,
            }),
        };
        assert_eq!(
            serde_json::to_value(&event).unwrap(),
            serde_json::json!({
                "type": "plugin_expiry_soon",
                "node_id": 7,
                "name": "edge-1",
                "expires_at": "2026-10-01",
                "days_left": 7,
                "threshold_days": 7,
            })
        );
        let event =
            Event::AgentOffline { node_id: 5, name: "edge-1".into(), observed_at: 100, last_seen_at: 90 };
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"type":"agent_offline","node_id":5,"name":"edge-1","observed_at":100,"last_seen_at":90}"#
        );
        let event = Event::AgentOnline { node_id: 5, name: "edge-1".into(), observed_at: 300 };
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"type":"agent_online","node_id":5,"name":"edge-1","observed_at":300}"#
        );
        let event = Event::NodeAdded { node_id: 5, name: "edge-1".into(), created_at: 100 };
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"type":"node_added","node_id":5,"name":"edge-1","created_at":100}"#
        );
        let event = Event::NodeDeleted { node_id: 5, name: "edge-1".into(), created_at: 100 };
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"type":"node_deleted","node_id":5,"name":"edge-1","created_at":100}"#
        );
    }
}
