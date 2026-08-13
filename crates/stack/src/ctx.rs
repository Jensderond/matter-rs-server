//! The single-threaded state every stack-thread task shares, plus the two
//! error-shaping helpers all IM operations funnel through.
//!
//! `RefCell` rather than a lock everywhere: rs-matter futures are `!Send` and
//! all of them run on one OS thread with a local executor, so there is no
//! contention to guard against — only the aliasing rules, which `RefCell`
//! enforces at the cost of keeping every borrow short (never across an
//! `.await`).

// TODO(task16): remove — Task 15 (commissioning/window/fabrics/discovery) and
// Task 16 (runtime, supervisor, StackHandle) are the consumers of most of this.
// While this is here it also suppresses genuine dead-code findings, e.g. an
// `addrs` map that never gets a writer.
#![allow(dead_code)]

use core::future::Future;
use core::num::NonZeroU8;
use core::pin::pin;
use std::cell::RefCell;
use std::collections::HashMap;

use embassy_futures::select::{select, Either};
use embassy_time::{Duration, Instant, Timer};

use matter_rs_controller::stack_api::{StackError, StackErrorKind, StackEvent};
use matter_rs_controller::storage::ServerIdentity;
use rs_matter::crypto::Crypto;
use rs_matter::error::{Error, ErrorCode};
use rs_matter::Matter;

/// Node-compatible wording: a failed commissioning attempt leaves the device's
/// failsafe armed, and until it expires the device rejects everything with
/// `Busy`. Users hit this constantly, so the message names the cause.
pub(crate) const BUSY_MESSAGE: &str =
    "device is busy (a previous commissioning attempt may still hold its failsafe for ~60s)";

/// Budget for a single IM transaction (read/write/invoke).
pub(crate) const IM_TIMEOUT_SECS: u64 = 30;
/// Budget for a full wildcard read. A bridge with dozens of endpoints chunks a
/// long time.
pub(crate) const INTERVIEW_TIMEOUT_SECS: u64 = 120;
/// Budget for one commissioning attempt, matching the device-side failsafe.
pub(crate) const COMMISSION_TIMEOUT_SECS: u64 = 60;

pub(crate) struct StackCtx<C: Crypto> {
    pub matter: &'static Matter<'static>,
    pub crypto: C,
    pub fab_idx: NonZeroU8,
    pub identity: ServerIdentity,
    pub events: tokio::sync::mpsc::UnboundedSender<StackEvent>,
    /// node_id -> the subscription id we established with that node.
    ///
    /// Keyed on the node, not the subscription id, because subscription ids are
    /// chosen by the *publisher*: two devices may both pick 1, and a single
    /// `subscription_id -> node_id` map would let the second insert steal the
    /// first node's reports (and let any node on the fabric forge attribute
    /// values for another by guessing an id). The report handler looks up by the
    /// CASE-authenticated `peer_node_id` and then checks the id matches, so both
    /// halves of the key are things we can trust or we chose. One entry per node
    /// is exact: the supervisor maintains exactly one wildcard subscription per
    /// node, and reports for a superseded id are answered `InvalidSubscription`,
    /// which is how the device is told to drop it.
    pub subs: RefCell<HashMap<u64, u32>>,
    /// node_id -> last report instant (liveness)
    pub liveness: RefCell<HashMap<u64, Instant>>,
    /// node_id -> highest event number already forwarded. Absent means "nothing
    /// seen yet", which is *not* the same as `Some(0)`: event number 0 is a
    /// legal first event and must not be mistaken for the initial state.
    pub last_event: RefCell<HashMap<u64, u64>>,
    /// node_id -> last known addresses ("ip" strings, most recent first)
    pub addrs: RefCell<HashMap<u64, Vec<String>>>,
    /// node_id -> supervisor task (dropping cancels)
    pub supervisors: RefCell<HashMap<u64, async_executor::Task<()>>>,
}

impl<C: Crypto> StackCtx<C> {
    pub fn new(
        matter: &'static Matter<'static>,
        crypto: C,
        fab_idx: NonZeroU8,
        identity: ServerIdentity,
        events: tokio::sync::mpsc::UnboundedSender<StackEvent>,
    ) -> Self {
        Self {
            matter,
            crypto,
            fab_idx,
            identity,
            events,
            subs: RefCell::new(HashMap::new()),
            liveness: RefCell::new(HashMap::new()),
            last_event: RefCell::new(HashMap::new()),
            addrs: RefCell::new(HashMap::new()),
            supervisors: RefCell::new(HashMap::new()),
        }
    }

    /// Record `event_number` as forwarded for `node_id`, returning `false` if it
    /// was already seen.
    pub fn note_event(&self, node_id: u64, event_number: u64) -> bool {
        note_event(&mut self.last_event.borrow_mut(), node_id, event_number)
    }

    /// Forget the event high-water mark for `node_id`.
    ///
    /// Task 15's supervisor MUST call this every time it establishes a
    /// subscription. The dedupe assumes event numbers only ever move forward, but
    /// a device that does not persist its event counter restarts numbering at 0
    /// after a reboot — and from then on every event compares `<=` the pre-reboot
    /// high-water mark and is dropped, permanently, until the controller
    /// restarts. matter.js resets per subscription establishment for exactly this
    /// reason.
    pub fn reset_event_high_water(&self, node_id: u64) {
        self.last_event.borrow_mut().remove(&node_id);
    }
}

/// Subscription-report dedupe: `true` when `event_number` has not been forwarded
/// for `node_id` yet. Devices replay events across resubscribes, and the event
/// counter may jump forward (but never backwards) on a device reboot.
///
/// Absence in the map — not a zero value — is the "nothing seen yet" state, so
/// that an event genuinely numbered 0 is forwarded exactly once. A device whose
/// counter goes *backwards* (a reboot without persistence) is not detectable
/// here; that is what [`StackCtx::reset_event_high_water`] is for.
pub(crate) fn note_event(last: &mut HashMap<u64, u64>, node_id: u64, event_number: u64) -> bool {
    match last.get(&node_id) {
        Some(seen) if event_number <= *seen => false,
        _ => {
            last.insert(node_id, event_number);
            true
        }
    }
}

/// rs-matter error -> the five wire-visible kinds the controller knows about.
///
/// The `Sdk` message is `Error::<code>` (plus any attached detail), which is
/// what `Debug` prints when rs-matter's `backtrace` feature is off. It is
/// spelled via `Display` deliberately: our dependency graph turns `backtrace`
/// *on* (it rides along on rs-matter's default `os` feature), and there
/// `Debug` appends a whole captured backtrace — which would end up verbatim in
/// the `details` field of a WS error response.
pub(crate) fn map_err(e: Error) -> StackError {
    match e.code() {
        // The transport reports an unresolvable peer this way: no mDNS record,
        // so no address to open a CASE session to.
        ErrorCode::NotFound => {
            StackError::new(StackErrorKind::NodeUnreachable, "could not resolve node via mDNS")
        }
        ErrorCode::RxTimeout | ErrorCode::TxTimeout => {
            StackError::new(StackErrorKind::Timeout, format!("Error::{e}"))
        }
        ErrorCode::Busy => StackError::new(StackErrorKind::Busy, BUSY_MESSAGE),
        _ => StackError::new(StackErrorKind::Sdk, format!("Error::{e}")),
    }
}

/// Run `fut` under a wall-clock budget, mapping both outcomes onto `StackError`.
///
/// rs-matter's own MRP retries can outlive any single caller's patience (a
/// device that ACKs but never answers keeps an exchange open), so every IM
/// operation needs an outer bound. Ported from the spike's `with_timeout`
/// (`spike/src/main.rs:404`), with the timeout surfacing as `Timeout` instead
/// of a synthetic `RxTimeout`.
pub(crate) async fn with_timeout<T>(
    secs: u64,
    fut: impl Future<Output = Result<T, Error>>,
) -> Result<T, StackError> {
    let fut = pin!(fut);
    let timer = pin!(Timer::after(Duration::from_secs(secs)));

    match select(fut, timer).await {
        Either::First(r) => r.map_err(map_err),
        Either::Second(()) => {
            let message = format!("IM operation timed out after {secs}s");
            tracing::warn!("{message}");
            Err(StackError::new(StackErrorKind::Timeout, message))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use embassy_futures::block_on;

    #[test]
    fn error_kinds() {
        assert_eq!(
            map_err(ErrorCode::NotFound.into()).kind,
            StackErrorKind::NodeUnreachable
        );
        assert_eq!(
            map_err(ErrorCode::NotFound.into()).message,
            "could not resolve node via mDNS"
        );
        assert_eq!(map_err(ErrorCode::RxTimeout.into()).kind, StackErrorKind::Timeout);
        assert_eq!(map_err(ErrorCode::TxTimeout.into()).kind, StackErrorKind::Timeout);
        assert_eq!(map_err(ErrorCode::Busy.into()).kind, StackErrorKind::Busy);
        // Compat-critical: users see this string when a previous commissioning
        // attempt left the failsafe armed.
        assert_eq!(map_err(ErrorCode::Busy.into()).message, BUSY_MESSAGE);
        assert_eq!(map_err(ErrorCode::Invalid.into()).kind, StackErrorKind::Sdk);
        assert_eq!(map_err(ErrorCode::Invalid.into()).message, "Error::Invalid");
        // InvalidData is only InvalidArguments when it comes from *our* payload
        // encoding; the operations detect that structurally, so the generic
        // mapping stays Sdk.
        assert_eq!(map_err(ErrorCode::InvalidData.into()).kind, StackErrorKind::Sdk);
        assert_eq!(map_err(ErrorCode::NoSession.into()).kind, StackErrorKind::Sdk);
    }

    #[test]
    fn timeout_reports_the_budget_it_blew() {
        let e = block_on(with_timeout(1, core::future::pending::<Result<(), Error>>()))
            .expect_err("pending future must time out");
        assert_eq!(e.kind, StackErrorKind::Timeout);
        assert_eq!(e.message, "IM operation timed out after 1s");
    }

    #[test]
    fn value_passes_through() {
        let v = block_on(with_timeout(30, core::future::ready(Ok::<u8, Error>(7))))
            .expect("ready future must pass through");
        assert_eq!(v, 7);
    }

    #[test]
    fn inner_error_is_mapped_not_swallowed() {
        let e = block_on(with_timeout(
            30,
            core::future::ready(Err::<(), Error>(ErrorCode::Busy.into())),
        ))
        .expect_err("inner error must surface");
        assert_eq!(e.kind, StackErrorKind::Busy);
    }

    #[test]
    fn event_zero_is_not_the_never_seen_sentinel() {
        let mut last = HashMap::new();
        assert!(note_event(&mut last, 1, 0), "event number 0 is a real first event");
        assert!(!note_event(&mut last, 1, 0), "and must not be forwarded twice");
        assert!(note_event(&mut last, 1, 1));
        assert!(!note_event(&mut last, 1, 1));
        // Replay after a resubscribe is dropped; a forward jump (device reboot)
        // is not.
        assert!(!note_event(&mut last, 1, 0));
        assert!(note_event(&mut last, 1, 9_000));
        // Per-node bookkeeping: another node starts from scratch.
        assert!(note_event(&mut last, 2, 0));
    }

    #[test]
    fn a_counter_reset_needs_the_high_water_mark_cleared() {
        let mut last = HashMap::new();
        assert!(note_event(&mut last, 1, 5_000));
        // Device reboots without persisting its counter and restarts at 0: every
        // event now looks like a replay and is dropped forever...
        assert!(!note_event(&mut last, 1, 0));
        assert!(!note_event(&mut last, 1, 1));
        // ...until the supervisor clears the mark on resubscribe.
        last.remove(&1);
        assert!(note_event(&mut last, 1, 0));
        assert!(note_event(&mut last, 1, 1));
    }
}
