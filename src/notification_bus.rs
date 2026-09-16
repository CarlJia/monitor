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
#[derive(Debug, Clone)]
pub enum Event {
    AgentOffline { node_id: i64, name: String, observed_at: i64, last_seen_at: i64 },
    AgentOnline { node_id: i64, name: String, observed_at: i64 },
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
    /// v2 支持的全部宿主自身事件名,manifest 的 `subscribes` 逐项对照。
    pub const KNOWN: [&'static str; 2] = [Self::AGENT_OFFLINE, Self::AGENT_ONLINE];

    /// The discriminator stored in `notification_log.event_type` and carried in
    /// the JSON `type` tag. A lifetime `&'static str` rather than a String:
    /// it is compared against database rows on every emission.
    pub fn type_name(&self) -> &str {
        match self {
            Event::AgentOffline { .. } => Self::AGENT_OFFLINE,
            Event::AgentOnline { .. } => Self::AGENT_ONLINE,
            Event::Plugin { name, .. } => name,
        }
    }

    pub fn node_id(&self) -> i64 {
        match self {
            Event::AgentOffline { node_id, .. } | Event::AgentOnline { node_id, .. } => *node_id,
            Event::Plugin { payload, .. } => payload.get("node_id").and_then(|v| v.as_i64()).unwrap_or(0),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Event::AgentOffline { name, .. } | Event::AgentOnline { name, .. } => name,
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
            Event::Plugin { name, payload } => {
                let threshold = payload.get("threshold_days").and_then(|v| v.as_i64()).unwrap_or(0);
                let day = payload
                    .get("expires_at")
                    .and_then(|v| v.as_str())
                    .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
                    .map(|d| d.num_days_from_ce() as i64)
                    .unwrap_or(0);
                let base = threshold * 1_000_000 + day;
                if base == 0 {
                    use std::hash::{Hash, Hasher};
                    let mut h = std::collections::hash_map::DefaultHasher::new();
                    name.hash(&mut h);
                    h.finish() as i64
                } else {
                    base
                }
            }
        }
    }

    /// True for the two sides of connectivity. State events are deduplicated
    /// by transition rather than by a content key: an offline node flapping
    /// its connection is one alert, not a stream of them.
    pub fn is_state_event(&self) -> bool {
        matches!(self, Event::AgentOffline { .. } | Event::AgentOnline { .. })
    }
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
    }
}
