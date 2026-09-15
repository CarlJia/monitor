//! WASM plugin runtime. U2 ships only the Registry stub the notification bus
//! calls into; the wasmtime host and dispatch machinery arrive with U3/U4.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::notification_bus::Event;

/// Placeholder until U3/U4: dispatch is a no-op, so event sources can be
/// built and tested before the runtime exists. The counter exists because the
/// stub has no state to inspect and the bus's tests need to confirm an
/// emission was not only recorded but forwarded; U3/U4 replace this struct
/// with the real runtime, whose dispatch results are visible in
/// `notification_log` directly.
#[derive(Default)]
pub struct Registry {
    dispatched: AtomicUsize,
}

impl Registry {
    pub fn dispatch(&self, _event: &Event) {
        self.dispatched.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dispatch_count(&self) -> usize {
        self.dispatched.load(Ordering::Relaxed)
    }
}
