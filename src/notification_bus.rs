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
use serde::Serialize;

use crate::App;

/// The three event types v1 ships with. Field set is what a plugin needs to
/// render a notification a human can act on.
///
/// Serialized with a `type` tag, so the payload handed to a plugin's
/// `on_event` reads `{"type":"agent_offline","node_id":1,...}` -- a shape
/// stable across plugin versions, since a WASM guest deserializes by field
/// name and tolerates additions.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    AgentOffline { node_id: i64, name: String, observed_at: i64, last_seen_at: i64 },
    AgentOnline { node_id: i64, name: String, observed_at: i64 },
    ExpirySoon { node_id: i64, name: String, expires_at: String, days_left: i64, threshold_days: i64 },
}

impl Event {
    /// The discriminator stored in `notification_log.event_type` and carried in
    /// the JSON `type` tag. A lifetime `&'static str` rather than a String:
    /// it is compared against database rows on every emission.
    pub fn type_name(&self) -> &'static str {
        match self {
            Event::AgentOffline { .. } => "agent_offline",
            Event::AgentOnline { .. } => "agent_online",
            Event::ExpirySoon { .. } => "expiry_soon",
        }
    }

    pub fn node_id(&self) -> i64 {
        match self {
            Event::AgentOffline { node_id, .. }
            | Event::AgentOnline { node_id, .. }
            | Event::ExpirySoon { node_id, .. } => *node_id,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Event::AgentOffline { name, .. }
            | Event::AgentOnline { name, .. }
            | Event::ExpirySoon { name, .. } => name,
        }
    }

    /// The idempotency key stored in `notification_log.threshold_or_state_key`.
    ///
    /// State events return 0: they hold one mutable row per node, and the
    /// row's identity is the node alone. `ExpirySoon` encodes
    /// `threshold_days * 1_000_000 + expires_at 的自公历纪元起的天数`
    /// (`NaiveDate::num_days_from_ce`): the tier keeps the thresholds from
    /// colliding with one another, and the expiry date makes the key specific
    /// to this billing cycle, so a renewal that rolls the date forward lets
    /// the same tier fire again. `num_days_from_ce` rather than a Unix day so
    /// the encoding needs no timezone choice -- it only has to differ per
    /// date, never be a wall-clock figure. An unparseable date contributes 0,
    /// which still keeps tiers distinct rather than collapsing them.
    pub fn threshold_or_state_key(&self) -> i64 {
        match self {
            Event::AgentOffline { .. } | Event::AgentOnline { .. } => 0,
            Event::ExpirySoon { threshold_days, expires_at, .. } => {
                let day = NaiveDate::parse_from_str(expires_at, "%Y-%m-%d")
                    .map(|d| d.num_days_from_ce() as i64)
                    .unwrap_or(0);
                threshold_days * 1_000_000 + day
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
        // transition also clears the opposite side's row in the same
        // transaction, so `current_state_event` is authoritative.
        if app
            .db
            .current_state_event(event.node_id())?
            .is_some_and(|(current, _)| current == event.type_name())
        {
            return Ok(());
        }
        if !app.db.transition_state_event(event.node_id(), event.type_name(), Utc::now().timestamp())? {
            return Ok(()); // Lost a race with a concurrent emission of the same side.
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
        Event::ExpirySoon {
            node_id,
            name: "edge-1".into(),
            expires_at: expires_at.into(),
            days_left,
            threshold_days,
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
        assert!(recorded(&app, 7, "expiry_soon", key), "the first emission records");
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
        assert!(recorded(&app, 7, "expiry_soon", renewed.threshold_or_state_key()));
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
        let event = Event::ExpirySoon {
            node_id: 7,
            name: "edge-1".into(),
            expires_at: "2026-10-01".into(),
            days_left: 7,
            threshold_days: 7,
        };
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"type":"expiry_soon","node_id":7,"name":"edge-1","expires_at":"2026-10-01","days_left":7,"threshold_days":7}"#
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
