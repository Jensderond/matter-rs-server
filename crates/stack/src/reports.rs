//! Decoding `ReportData` — both the solicited kind (read/subscribe responses,
//! via [`AttrAccumulator`]) and the unsolicited kind (post-priming subscription
//! reports, via [`ReportSink`]).
//!
//! `ReportSink` is registered via `InteractionModel::new_with_reports` by
//! `crate::runtime::run_stack`. Reports for `(node, subscription id)` pairs we do not track are
//! answered `InvalidSubscription`, which is what makes a device tear down a
//! subscription left over from before a controller restart — intended, not a
//! failure path.
//!
//! **Ordering contract for the supervisor:** that "intended" only holds if the
//! `subs` entry is inserted as early as the subscription id is knowable — the
//! instant `SubscribeOutcome::Established` is observed, with nothing awaited in
//! between (`supervisor::establish`, rule 1). A device that reports immediately
//! would otherwise be told to tear down the subscription it just established.
//!
//! Inserting it *during* the subscribe exchange would be stricter, and is
//! **impossible at this rev** — do not go looking for a way.
//! `SubscribePrimingChunk::complete`
//! (`rs-matter-ref/rs-matter/src/im/client.rs:1293-1307`) reads `subs_id`, awaits
//! `exchange.acknowledge()`, and *drops the exchange* before returning
//! `Established`, so the id first becomes visible to us after the exchange is
//! already gone. The residual window is that `acknowledge()`-to-return interval:
//! sub-millisecond, and a device fast enough to report inside it gets one
//! `InvalidSubscription`, drops its half, and is picked up by the watchdog on the
//! next cycle — a clean recovery precisely because the resubscribe sends
//! `keep_subs(false)`.

use std::collections::HashMap;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use matter_rs_controller::stack_api::{NodeEventData, StackEvent};
use rs_matter::crypto::Crypto;
use rs_matter::dm::{ReportContext, ReportDataHandler};
use rs_matter::im::{
    AttrPath, AttrResp, EventData, EventDataTimestamp, EventResp, IMStatusCode, ReportDataResp,
};
use rs_matter::tlv::TLVElement;
use serde_json::Value;

use crate::ctx::StackCtx;
use crate::tlv_json;

/// Collects attribute reports from one or more `ReportData` chunks into ordered
/// `("endpoint/cluster/attribute", JSON)` pairs.
///
/// Exists because three call sites need the identical walk: `read_attributes`,
/// Task 15's subscribe-priming loop, and `ReportSink` below. All three also want
/// the same error policy — log and skip. A wildcard interview reads thousands of
/// paths, and one attribute the device encodes badly (a nullable `epoch_us` sent
/// as `0xFFFF_FFFF_FFFF_FFFF` rather than TLV Null overflows `apply_epoch`, for
/// instance) must not discard every other attribute on the node.
///
/// The non-obvious job is reassembling chunked lists. When a list attribute does
/// not fit the remaining TX space, devices split it into N+1
/// `AttributeReportIB`s that all carry the *same* endpoint/cluster/attribute:
/// first an empty array with **no `ListIndex` field at all** (a whole-value
/// replace — this is also how an ordinary unchunked attribute arrives), then
/// one report per element with `ListIndex` **present and `null`** (append).
/// This is the spec's wire encoding, and it is *not* what
/// `send_array_items` (`rs-matter-ref/rs-matter/src/im.rs:2131-2158`) looks
/// like it does at a glance: that loop's own `attr.list_index` walks concrete
/// indices `Some(0), Some(1), …`, but that field only selects which element
/// to *read* from the underlying store. What actually reaches the wire is
/// `AttrDetails::reply_path` (`rs-matter-ref/rs-matter/src/dm/types/attribute.rs:299-317`),
/// which translates every concrete index to `Nullable::none()` (present,
/// null) before it is serialized — the ground truth, and the thing to trust
/// over the internal loop. Keyed on the path alone, and dispatched on the
/// wrong half of that translation, appends collapse last-wins, and a
/// 40-entry `PartsList` arrives at the client as a bare integer.
#[derive(Debug, Default)]
pub(crate) struct AttrAccumulator {
    /// Wire order is preserved; the index only exists to find the slot to merge
    /// a list element into.
    out: Vec<(String, Value)>,
    index: HashMap<String, usize>,
    orphan_appends: Vec<String>,
    saw_list_chunks: bool,
    failures: usize,
}

impl AttrAccumulator {
    /// Absorb the `attr_reports` of one chunk. `who` prefixes log lines.
    pub fn absorb(&mut self, report: &ReportDataResp<'_>, who: &str) {
        let Some(reports) = &report.attr_reports else {
            return;
        };

        for r in reports.iter() {
            let r = match r {
                Ok(r) => r,
                // Dropping this silently would make a device sending garbage
                // indistinguishable from one sending nothing.
                Err(e) => {
                    self.failures += 1;
                    tracing::warn!("{who}: unparseable attribute report: {e}");
                    continue;
                }
            };

            let data = match r {
                AttrResp::Data(data) => data,
                // Expected on wildcard reads: unsupported or access-denied paths
                // come back as a per-path status.
                AttrResp::Status(s) => {
                    tracing::debug!("{who}: path status {:?} for {:?}", s.status, s.path);
                    continue;
                }
            };

            // Read the path fields directly rather than via `to_gp()`, which
            // drops `list_index` (`im/encoding/attr.rs:105`) — the very field the
            // list merge turns on.
            let (Some(e), Some(cl), Some(a)) =
                (data.path.endpoint, data.path.cluster, data.path.attr)
            else {
                self.failures += 1;
                tracing::warn!("{who}: report with wildcard path {:?}", data.path);
                continue;
            };

            let json = match tlv_json::attr_value_to_json(cl, a, &data.data) {
                Ok(json) => json,
                Err(err) => {
                    self.failures += 1;
                    tracing::warn!("{who}: cannot convert {e}/{cl}/{a}: {err}");
                    continue;
                }
            };

            let key = format!("{e}/{cl}/{a}");
            match list_op(&data.path) {
                ListOp::Replace => self.put(key, json),
                ListOp::Append => {
                    self.saw_list_chunks = true;
                    self.append(key, json, who);
                }
                ListOp::SetIndex(i) => {
                    self.saw_list_chunks = true;
                    self.set_index(key, i, json, who);
                }
            }
        }
    }

    /// A whole attribute value: replaces anything already recorded for the path,
    /// which is also how the empty array that opens a chunked list arrives.
    fn put(&mut self, key: String, json: Value) {
        match self.index.get(&key) {
            Some(&at) => {
                if let Some(slot) = self.out.get_mut(at) {
                    slot.1 = json;
                }
            }
            None => {
                self.index.insert(key.clone(), self.out.len());
                self.out.push((key, json));
            }
        }
    }

    /// One element of a chunked list, appended to the array opened by the
    /// preceding no-`ListIndex` report.
    ///
    /// No index accompanies an append (`list_op`'s doc — every appended
    /// element is tagged `ListIndex = null` on the wire, never a concrete
    /// number), so there is nothing to gap-check against: wire order is
    /// append order, full stop.
    fn append(&mut self, key: String, json: Value, who: &str) {
        let Some(&at) = self.index.get(&key) else {
            // No array to append to. Either the report is malformed, or — the
            // real case — this is a subscription report whose list started in a
            // previous `ReportData` *message*, which arrives as a separate
            // `handle_report` call. Cross-message merging is not implemented, so
            // say so loudly instead of handing the client a truncated array.
            tracing::warn!(
                "{who}: list append for {key} has no array to append to; the list is split \
                 across ReportData messages and is reported incomplete"
            );
            self.orphan_appends.push(key.clone());
            self.index.insert(key.clone(), self.out.len());
            self.out.push((key, Value::Array(vec![json])));
            return;
        };

        let Some(slot) = self.out.get_mut(at) else {
            return;
        };
        match &mut slot.1 {
            Value::Array(arr) => arr.push(json),
            other => {
                tracing::warn!(
                    "{who}: list append for {} arrived after a non-array value; restarting the \
                     array",
                    slot.0
                );
                *other = Value::Array(vec![json]);
            }
        }
    }

    /// Set element `i` of the array recorded for `key`, per the spec-legal but
    /// rare "replace one element by index" report. Not what any chunker in
    /// this codebase's test devices has been observed to send (see
    /// `list_op`'s doc) — chunked lists always append via a null index — but
    /// a concrete index is legal on the wire, so it must not be misread as an
    /// append (which would silently shift every later element by one).
    fn set_index(&mut self, key: String, i: u16, json: Value, who: &str) {
        let at = match self.index.get(&key) {
            Some(&at) => at,
            None => {
                self.index.insert(key.clone(), self.out.len());
                self.out.push((key, Value::Array(Vec::new())));
                self.out.len() - 1
            }
        };

        let slot = &mut self.out[at];
        if !matches!(slot.1, Value::Array(_)) {
            tracing::warn!(
                "{who}: indexed list element {i} for {} arrived after a non-array value; \
                 restarting the array",
                slot.0
            );
            slot.1 = Value::Array(Vec::new());
        }
        let Value::Array(arr) = &mut slot.1 else { unreachable!("just ensured above") };

        let idx = usize::from(i);
        if idx >= arr.len() {
            // A concrete index ahead of the array's current end: pad with
            // `null` rather than refusing the report, mirroring how a JSON
            // array literal with a gap would be read back.
            arr.resize(idx + 1, Value::Null);
        }
        arr[idx] = json;
    }

    pub fn into_pairs(self) -> Vec<(String, Value)> {
        self.out
    }

    /// Paths whose list elements could not be merged (see [`Self::append`]).
    ///
    /// Test-only: in production the incomplete list is reported by `append`'s own
    /// warning at the moment it happens, so no caller needs to ask afterwards.
    #[cfg(test)]
    pub fn orphan_appends(&self) -> &[String] {
        &self.orphan_appends
    }

    /// Whether any report in this batch was part of a chunked list.
    pub fn saw_list_chunks(&self) -> bool {
        self.saw_list_chunks
    }

    /// Reports logged and skipped rather than returned.
    pub fn failures(&self) -> usize {
        self.failures
    }
}

/// What an `AttributeDataIB`'s path says to do with its `Data`, per
/// `AttrPath::list_index: Option<Nullable<u16>>` (`im/encoding/attr.rs:72`) —
/// three wire states, three different actions.
///
/// Ground truth is `AttrDetails::reply_path`
/// (`rs-matter-ref/rs-matter/src/dm/types/attribute.rs:299-317`), the
/// function that actually builds this path for the wire:
/// - field **absent** → [`ListOp::Replace`]. Both an ordinary unchunked
///   attribute and the empty array that opens a chunked list arrive this way
///   (`reply_path`'s `Some(None) | None => None` arm folds its own
///   "give me the whole list" request into the same absent field a scalar
///   attribute gets).
/// - field present, **null** → [`ListOp::Append`]. `reply_path`'s
///   `Some(Some(_)) => Some(Nullable::none())` arm is the whole point: every
///   concrete index `send_array_items` (`im.rs:2131-2158`) walks internally
///   to pick which element to *read* is rewritten to null before it is
///   serialized, so a real device's per-element chunk never carries a
///   number — only the append signal.
/// - field present, a **concrete index** → [`ListOp::SetIndex`]. Legal per
///   spec (replace one element in place) but not what this codebase's list
///   chunking ever emits — see above. Kept distinct from `Append` so a
///   concrete index can never be misread as one, which would shift every
///   later element by one instead of replacing where it lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ListOp {
    Replace,
    Append,
    SetIndex(u16),
}

fn list_op(path: &AttrPath) -> ListOp {
    match path.list_index.as_ref().map(|n| n.as_opt_ref()) {
        None => ListOp::Replace,
        Some(None) => ListOp::Append,
        Some(Some(&i)) => ListOp::SetIndex(i),
    }
}

/// The `ReportDataHandler` the runtime registers via
/// `InteractionModel::new_with_reports`: every inbound subscription report lands
/// here and is forwarded to the controller as a `StackEvent`.
pub(crate) struct ReportSink<C: Crypto>(pub Rc<StackCtx<C>>);

impl<C: Crypto> ReportDataHandler for ReportSink<C> {
    async fn handle_report(
        &self,
        rctx: impl ReportContext,
        report: &ReportDataResp<'_>,
    ) -> Result<(), IMStatusCode> {
        let ctx = &self.0;
        let sub = rctx.subscription();

        let Some(sub_id) = sub.subscription_id else {
            tracing::debug!("subscription-less ReportData; disowning");
            return Err(IMStatusCode::InvalidSubscription);
        };
        // Subscription ids are chosen by the *publisher*, so an id on its own is
        // neither unique across nodes nor trustworthy. `peer_node_id` and
        // `fabric_idx` come from the CASE session the report arrived on, so they
        // are authenticated: match the id against what we recorded for that node
        // and nothing can be injected on another node's behalf.
        if sub.fabric_idx != ctx.fab_idx {
            tracing::debug!(
                "report on fabric {} but we operate fabric {}; disowning",
                sub.fabric_idx,
                ctx.fab_idx
            );
            return Err(IMStatusCode::InvalidSubscription);
        }
        let node_id = sub.peer_node_id;
        if ctx.subs.borrow().get(&node_id) != Some(&sub_id) {
            tracing::debug!("report for untracked subscription {sub_id} on node {node_id}; disowning");
            return Err(IMStatusCode::InvalidSubscription);
        }

        // Any report at all proves the device is alive, even one we then fail to
        // convert — the watchdog cares about traffic, not content.
        ctx.liveness.borrow_mut().insert(node_id, embassy_time::Instant::now());

        let who = format!("node {node_id}");
        let mut acc = AttrAccumulator::default();
        acc.absorb(report, &who);
        if acc.saw_list_chunks() && report.more_chunks == Some(true) {
            tracing::warn!(
                "{who}: chunked list continues in a following ReportData message; the merged \
                 value may be incomplete"
            );
        }
        let changes = acc.into_pairs();
        if !changes.is_empty() {
            // A closed receiver means the controller is shutting down.
            let _ = ctx.events.send(StackEvent::AttributesChanged { node_id, changes });
        }

        walk_events(report, &who, |data| {
            if !ctx.note_event(node_id, data.event_number) {
                return; // replayed across a resubscribe
            }
            let _ = ctx.events.send(StackEvent::NodeEvent { node_id, event: node_event(data) });
        });

        Ok(())
    }
}

/// Hand every parsed `EventData` in a report to `f`, in wire order.
///
/// Shared with `supervisor::establish`, whose priming loop only records event
/// numbers where this handler records *and* forwards. The triage is what is worth
/// sharing rather than the action: a per-path `Status` is expected (an event the
/// device will not disclose) and belongs at debug, while an entry that will not
/// parse is a device bug and belongs at warn — and both must be skipped rather
/// than aborting the walk, or one bad event costs every later one in the report.
pub(crate) fn walk_events(
    report: &ReportDataResp<'_>,
    who: &str,
    mut f: impl FnMut(&EventData<'_>),
) {
    let Some(events) = &report.event_reports else {
        return;
    };
    for r in events.iter() {
        match r {
            Ok(EventResp::Data(data)) => f(&data),
            Ok(EventResp::Status(s)) => {
                tracing::debug!("{who}: event status {:?} for {:?}", s.status, s.path);
            }
            Err(e) => tracing::warn!("{who}: unparseable event report: {e}"),
        }
    }
}

fn node_event(data: &EventData<'_>) -> NodeEventData {
    let (timestamp, timestamp_type) = convert_timestamp(&data.timestamp);
    let cluster_id = data.path.cluster.unwrap_or(0);
    let event_id = data.path.event.unwrap_or(0);

    NodeEventData {
        endpoint_id: data.path.endpoint.unwrap_or(0),
        cluster_id,
        event_id,
        event_number: data.event_number,
        priority: data.priority as u8,
        timestamp,
        timestamp_type,
        data: event_json(cluster_id, event_id, &data.data),
    }
}

/// `(unix millis, type)` where type is Node's encoding: 1 = epoch, 0 = system
/// uptime, 2 = "we substituted our own clock".
///
/// `EventDataIB` tag 3 carries **Posix milliseconds since 1970**, not the
/// `epoch-us` (Matter-epoch microseconds) the plan assumed — settled on the
/// Task 19 device run and not guessable from rs-matter, which passes the raw
/// `u64` through without an opinion. Two independent confirmations:
///
/// 1. matter.js types the field `TlvPosixMs` (a bare `TlvUInt64`) in
///    `@matter/types/.../protocol/types/TlvEventData.js`, distinct from the
///    `TlvEpochUs` it has and does not use here, and writes its occurrence
///    store's `epochTimestamp` — Unix ms — into it unconverted.
/// 2. Wall clock: the virtual device logged a `BasicInformation.shutDown` with
///    `epochTimestamp: 1786698881562` (2026-08-14T09:14:41.562Z, the moment it was
///    interrupted) and this function now reports exactly 1786698881562. Under the
///    `epoch-us` reading the same value came out as 948471498881 — January 2000,
///    26 years off, which is what an earlier run reported to the client.
///
/// So this now matches both rs-matter's own doc comment
/// (`rs-matter-ref/rs-matter/src/im/encoding/event.rs:349`) and CHIP, which
/// stamps `Timestamp::Epoch` in milliseconds too. `MATTER_EPOCH_OFFSET_US` still
/// applies to `epoch-us` *attribute* fields (`tlv_json`) — that is a different
/// spec type, and only the event timestamp was misread.
///
/// Delta-encoded timestamps are relative to the previous event on the same
/// subscription, which we do not retain across chunks, so they fall back to the
/// local clock and say so via type 2.
fn convert_timestamp(ts: &EventDataTimestamp) -> (i64, u8) {
    match ts {
        // Same raw-`u64` forwarding as the system branch below, so the `i64`
        // clamp is equally reachable and must not go negative.
        EventDataTimestamp::EpochTimestamp(ms) => (i64::try_from(*ms).unwrap_or(i64::MAX), 1),
        EventDataTimestamp::SystemTimestamp(ms) => (i64::try_from(*ms).unwrap_or(i64::MAX), 0),
        EventDataTimestamp::DeltaEpochTimestamp(_)
        | EventDataTimestamp::DeltaSystemTimestamp(_) => (unix_millis(SystemTime::now()), 2),
    }
}

/// Unix millis, or 0 for a clock set before 1970 or beyond `i64`. A wrong
/// timestamp on one event is not worth failing the event over.
fn unix_millis(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

/// Event payloads go out name-based (camelCase from `gen`), like command
/// responses. An event `gen` does not know falls back to tag-based keys rather
/// than being dropped.
fn event_json(cluster_id: u32, event_id: u32, payload: &TLVElement<'_>) -> Value {
    if payload.is_empty() {
        return Value::Null;
    }
    let named = matter_rs_gen::cluster(cluster_id).and_then(|c| c.event(event_id).map(|e| (c, e)));
    let converted = match named {
        Some((cluster, event)) => tlv_json::tlv_to_json_named(payload, event.fields, cluster),
        None => tlv_json::tlv_to_json(payload),
    };
    match converted {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("cannot convert event {cluster_id}/{event_id} payload: {e}");
            Value::Null
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use rs_matter::im::AttrData;
    use rs_matter::tlv::{FromTLV, Nullable, TLVTag, TLVWrite, ToTLV};
    use rs_matter::utils::storage::WriteBuf;
    use serde_json::json;

    fn build(f: impl FnOnce(&mut WriteBuf<'_>)) -> Vec<u8> {
        let mut buf = [0u8; 512];
        let mut wb = WriteBuf::new(&mut buf);
        f(&mut wb);
        wb.as_slice().to_vec()
    }

    /// A concrete `AttrPath`, optionally carrying the `ListIndex` of a chunked
    /// list report.
    fn path(e: u16, cl: u32, a: u32, list_index: Option<Option<u16>>) -> AttrPath {
        AttrPath {
            endpoint: Some(e),
            cluster: Some(cl),
            attr: Some(a),
            list_index: list_index.map(Nullable::new),
            ..Default::default()
        }
    }

    /// Real `ReportDataMessage` wire bytes, so the accumulator is exercised
    /// through `FromTLV` exactly as it is on the network.
    fn report_bytes(entries: &[AttrResp<'_>], more_chunks: bool) -> Vec<u8> {
        build(|w| {
            w.start_struct(&TLVTag::Anonymous).unwrap();
            w.start_array(&TLVTag::Context(1)).unwrap(); // AttributeReports
            for e in entries {
                e.to_tlv(&TLVTag::Anonymous, &mut *w).unwrap();
            }
            w.end_container().unwrap();
            if more_chunks {
                w.bool(&TLVTag::Context(3), true).unwrap(); // MoreChunkedMsgs
            }
            w.end_container().unwrap();
        })
    }

    fn absorb(bytes: &[u8]) -> AttrAccumulator {
        let elem = TLVElement::new(bytes);
        let report = ReportDataResp::from_tlv(&elem).expect("parse ReportData");
        let mut acc = AttrAccumulator::default();
        acc.absorb(&report, "test");
        acc
    }

    #[test]
    fn unchunked_attribute_keeps_its_value() {
        let v = build(|w| w.u16(&TLVTag::Anonymous, 42).unwrap());
        let bytes = report_bytes(
            &[AttrResp::Data(AttrData::new(None, path(1, 6, 0, None), TLVElement::new(&v)))],
            false,
        );
        assert_eq!(absorb(&bytes).into_pairs(), vec![("1/6/0".to_string(), json!(42))]);
    }

    /// The bug this whole accumulator exists for: 3 elements of `0/29/3` used to
    /// collapse to the last one. Wire shape per `list_op`'s doc: the opener
    /// carries no `ListIndex` field at all, and every element after it carries
    /// `ListIndex = null` — never a concrete number, which is the mistake a
    /// prior version of this test (and the code it exercised) made.
    #[test]
    fn chunked_list_is_merged_back_into_an_array() {
        let empty = build(|w| {
            w.start_array(&TLVTag::Anonymous).unwrap();
            w.end_container().unwrap();
        });
        let e0 = build(|w| w.u16(&TLVTag::Anonymous, 11).unwrap());
        let e1 = build(|w| w.u16(&TLVTag::Anonymous, 22).unwrap());
        let e2 = build(|w| w.u16(&TLVTag::Anonymous, 33).unwrap());

        let bytes = report_bytes(
            &[
                // No ListIndex field: replaces with an empty array, opening the list.
                AttrResp::Data(AttrData::new(None, path(0, 29, 3, None), TLVElement::new(&empty))),
                // ListIndex = null, repeated: one append per element, never a
                // concrete index.
                AttrResp::Data(AttrData::new(
                    None,
                    path(0, 29, 3, Some(None)),
                    TLVElement::new(&e0),
                )),
                AttrResp::Data(AttrData::new(
                    None,
                    path(0, 29, 3, Some(None)),
                    TLVElement::new(&e1),
                )),
                AttrResp::Data(AttrData::new(
                    None,
                    path(0, 29, 3, Some(None)),
                    TLVElement::new(&e2),
                )),
            ],
            false,
        );

        let acc = absorb(&bytes);
        assert!(acc.saw_list_chunks());
        assert!(acc.orphan_appends().is_empty());
        assert_eq!(acc.into_pairs(), vec![("0/29/3".to_string(), json!([11, 22, 33]))]);
    }

    /// Priming (`supervisor::establish`) and the read/interview path
    /// (`ops::interact::read_attributes`) both share one accumulator across
    /// every chunk of a *whole* exchange, not just one `ReportData` message —
    /// so a list opened in message 1 and appended to in message 2 must still
    /// merge into one array, unlike `ReportSink`'s per-message accumulator
    /// (documented at [`AttrAccumulator::append`]).
    #[test]
    fn a_list_opened_in_one_message_and_appended_to_in_the_next_still_merges() {
        let empty = build(|w| {
            w.start_array(&TLVTag::Anonymous).unwrap();
            w.end_container().unwrap();
        });
        let items: Vec<Vec<u8>> =
            (0u16..16).map(|i| build(|w| w.u32(&TLVTag::Anonymous, u32::from(i)).unwrap())).collect();

        let msg1 = report_bytes(
            &[
                AttrResp::Data(AttrData::new(None, path(0, 29, 1, None), TLVElement::new(&empty))),
                AttrResp::Data(AttrData::new(None, path(0, 29, 1, Some(None)), TLVElement::new(&items[0]))),
                AttrResp::Data(AttrData::new(None, path(0, 29, 1, Some(None)), TLVElement::new(&items[1]))),
            ],
            true,
        );
        let msg2 = report_bytes(
            &items[2..]
                .iter()
                .map(|item| {
                    AttrResp::Data(AttrData::new(None, path(0, 29, 1, Some(None)), TLVElement::new(item)))
                })
                .collect::<Vec<_>>(),
            false,
        );

        let mut acc = AttrAccumulator::default();
        for bytes in [msg1, msg2] {
            let elem = TLVElement::new(&bytes);
            let report = ReportDataResp::from_tlv(&elem).expect("parse ReportData");
            acc.absorb(&report, "test");
        }
        let expected: Vec<Value> = (0..16).map(Value::from).collect();
        assert_eq!(acc.into_pairs(), vec![("0/29/1".to_string(), Value::Array(expected))]);
    }

    #[test]
    fn list_element_without_an_opening_array_is_flagged_not_hidden() {
        let e0 = build(|w| w.u16(&TLVTag::Anonymous, 11).unwrap());
        let bytes = report_bytes(
            &[AttrResp::Data(AttrData::new(
                None,
                path(0, 29, 3, Some(None)),
                TLVElement::new(&e0),
            ))],
            true,
        );
        let acc = absorb(&bytes);
        assert_eq!(acc.orphan_appends(), ["0/29/3"]);
        // The element still reaches the client rather than vanishing.
        assert_eq!(acc.into_pairs(), vec![("0/29/3".to_string(), json!([11]))]);
    }

    /// Requirement: a later whole-value replace (no `ListIndex` field) must
    /// discard a previously chunked-and-merged array, not merge into it —
    /// this is the normal way a small list's *next* value arrives once it no
    /// longer needs chunking.
    #[test]
    fn a_later_full_replace_replaces_the_merged_array() {
        let empty = build(|w| {
            w.start_array(&TLVTag::Anonymous).unwrap();
            w.end_container().unwrap();
        });
        let e0 = build(|w| w.u16(&TLVTag::Anonymous, 11).unwrap());
        let replacement = build(|w| {
            w.start_array(&TLVTag::Anonymous).unwrap();
            w.u16(&TLVTag::Anonymous, 99).unwrap();
            w.end_container().unwrap();
        });

        let bytes = report_bytes(
            &[
                AttrResp::Data(AttrData::new(None, path(0, 29, 3, None), TLVElement::new(&empty))),
                AttrResp::Data(AttrData::new(
                    None,
                    path(0, 29, 3, Some(None)),
                    TLVElement::new(&e0),
                )),
                // No ListIndex: a whole new value, unrelated to the array just built.
                AttrResp::Data(AttrData::new(
                    None,
                    path(0, 29, 3, None),
                    TLVElement::new(&replacement),
                )),
            ],
            false,
        );
        assert_eq!(absorb(&bytes).into_pairs(), vec![("0/29/3".to_string(), json!([99]))]);
    }

    /// The rare, spec-legal "set element by concrete index" report — distinct
    /// from an append, and must not be misread as one (which would shift
    /// every later element by a position instead of landing where the index
    /// says).
    #[test]
    fn concrete_index_sets_that_element_padding_gaps_with_null() {
        let e5 = build(|w| w.u16(&TLVTag::Anonymous, 55).unwrap());
        let bytes = report_bytes(
            &[AttrResp::Data(AttrData::new(
                None,
                path(0, 29, 3, Some(Some(5))),
                TLVElement::new(&e5),
            ))],
            false,
        );
        let acc = absorb(&bytes);
        assert!(acc.orphan_appends().is_empty(), "a concrete index is not an append");
        assert_eq!(
            acc.into_pairs(),
            vec![("0/29/3".to_string(), json!([null, null, null, null, null, 55]))]
        );
    }

    #[test]
    fn wire_order_is_preserved_across_a_merge() {
        let a = build(|w| w.u16(&TLVTag::Anonymous, 1).unwrap());
        let empty = build(|w| {
            w.start_array(&TLVTag::Anonymous).unwrap();
            w.end_container().unwrap();
        });
        let e0 = build(|w| w.u16(&TLVTag::Anonymous, 2).unwrap());
        let z = build(|w| w.u16(&TLVTag::Anonymous, 3).unwrap());

        let bytes = report_bytes(
            &[
                AttrResp::Data(AttrData::new(None, path(0, 40, 1, None), TLVElement::new(&a))),
                AttrResp::Data(AttrData::new(None, path(0, 29, 3, None), TLVElement::new(&empty))),
                AttrResp::Data(AttrData::new(
                    None,
                    path(0, 29, 3, Some(None)),
                    TLVElement::new(&e0),
                )),
                AttrResp::Data(AttrData::new(None, path(0, 40, 2, None), TLVElement::new(&z))),
            ],
            false,
        );
        let keys: Vec<String> = absorb(&bytes).into_pairs().into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, ["0/40/1", "0/29/3", "0/40/2"]);
    }

    /// A single unconvertible attribute must not discard the rest of a wildcard
    /// interview.
    #[test]
    fn one_bad_attribute_does_not_lose_the_others() {
        // 0/40/1 is BasicInformation.vendorName (char_string) — fine.
        let good = build(|w| w.utf8(&TLVTag::Anonymous, "acme").unwrap());
        // 6/47/2 (PowerSource) does not matter; what matters is that this path's
        // gen type is epoch_us so `apply_epoch` overflows on the u64 sentinel.
        let bad = build(|w| w.u64(&TLVTag::Anonymous, u64::MAX).unwrap());
        let epoch_us_path = epoch_us_attr();

        let bytes = report_bytes(
            &[
                AttrResp::Data(AttrData::new(
                    None,
                    path(epoch_us_path.0, epoch_us_path.1, epoch_us_path.2, None),
                    TLVElement::new(&bad),
                )),
                AttrResp::Data(AttrData::new(None, path(0, 40, 1, None), TLVElement::new(&good))),
            ],
            false,
        );
        let acc = absorb(&bytes);
        assert_eq!(acc.failures(), 1, "the bad attribute is counted, not fatal");
        assert_eq!(acc.into_pairs(), vec![("0/40/1".to_string(), json!("acme"))]);
    }

    /// Find any `(endpoint, cluster, attr)` whose `gen` type is `epoch_us`, so
    /// the overflow test does not hard-code a cluster that may be re-spelled.
    fn epoch_us_attr() -> (u16, u32, u32) {
        // TimeSynchronization (56) / UTCTime (0) is epoch_us in V1.6.0.0.
        let c = matter_rs_gen::cluster(56).expect("TimeSynchronization");
        let a = c.attr(0).expect("UTCTime");
        assert!(a.ty.eq_ignore_ascii_case("epoch_us"), "expected epoch_us, got {}", a.ty);
        (0, 56, 0)
    }

    #[test]
    fn per_path_status_entries_are_skipped_not_counted_as_failures() {
        use rs_matter::im::{AttrStatus, Status};
        let bytes = report_bytes(
            &[AttrResp::Status(AttrStatus {
                path: path(1, 6, 0, None),
                status: Status::new(IMStatusCode::UnsupportedAccess, None),
            })],
            false,
        );
        let acc = absorb(&bytes);
        assert_eq!(acc.failures(), 0);
        assert!(acc.into_pairs().is_empty());
    }

    #[test]
    fn wildcard_path_in_a_report_is_rejected() {
        let v = build(|w| w.u16(&TLVTag::Anonymous, 42).unwrap());
        let bytes = report_bytes(
            &[AttrResp::Data(AttrData::new(
                None,
                AttrPath { endpoint: Some(1), cluster: Some(6), ..Default::default() },
                TLVElement::new(&v),
            ))],
            false,
        );
        let acc = absorb(&bytes);
        assert_eq!(acc.failures(), 1);
        assert!(acc.into_pairs().is_empty());
    }

    /// The exact value the Task 19 device run produced: the virtual device's
    /// `shutDown` carried `epochTimestamp: 1786698881562` and must come out as
    /// that same instant, not as the January-2000 value the `epoch-us` reading
    /// would give it (948471498881).
    #[test]
    fn epoch_timestamp_is_posix_millis_not_matter_epoch_micros() {
        let (ms, ty) = convert_timestamp(&EventDataTimestamp::EpochTimestamp(1_786_698_881_562));
        assert_eq!((ms, ty), (1_786_698_881_562, 1));
        // 2024-01-01T00:00:00Z, for a second point that is easy to read.
        let (ms, _) = convert_timestamp(&EventDataTimestamp::EpochTimestamp(1_704_067_200_000));
        assert_eq!(ms, 1_704_067_200_000);
    }

    /// Forwarding the raw `u64` makes the `i64` clamp reachable, exactly as on the
    /// system branch: a device claiming a huge epoch must not yield a negative
    /// timestamp.
    #[test]
    fn epoch_timestamp_beyond_i64_saturates() {
        let (ms, ty) = convert_timestamp(&EventDataTimestamp::EpochTimestamp(u64::MAX));
        assert_eq!((ms, ty), (i64::MAX, 1));
    }

    #[test]
    fn system_timestamp_passes_through_as_type_zero() {
        let (ms, ty) = convert_timestamp(&EventDataTimestamp::SystemTimestamp(90_000));
        assert_eq!((ms, ty), (90_000, 0));
    }

    /// The sibling of `epoch_timestamp_beyond_i64_saturates`: since the epoch fix
    /// both branches forward the raw `u64`, so both have a reachable `i64` clamp
    /// and neither may produce a negative timestamp.
    #[test]
    fn system_timestamp_beyond_i64_saturates() {
        let (ms, ty) = convert_timestamp(&EventDataTimestamp::SystemTimestamp(u64::MAX));
        assert_eq!((ms, ty), (i64::MAX, 0));
    }

    #[test]
    fn delta_timestamps_fall_back_to_local_clock() {
        for ts in [
            EventDataTimestamp::DeltaEpochTimestamp(500),
            EventDataTimestamp::DeltaSystemTimestamp(500),
        ] {
            // Bracketed rather than compared against a fixed date, so the test
            // holds on a host whose clock is unset.
            let before = unix_millis(SystemTime::now());
            let (ms, ty) = convert_timestamp(&ts);
            let after = unix_millis(SystemTime::now());
            assert_eq!(ty, 2);
            assert!((before..=after).contains(&ms), "{ms} not in {before}..={after}");
        }
    }

    #[test]
    fn pre_epoch_clock_yields_zero_rather_than_a_negative_timestamp() {
        assert_eq!(unix_millis(UNIX_EPOCH), 0);
        let before_epoch = UNIX_EPOCH
            .checked_sub(Duration::from_secs(1))
            .expect("representable on this platform");
        assert_eq!(unix_millis(before_epoch), 0);
    }

    #[test]
    fn known_event_payload_is_named() {
        // BasicInformation (40) / StartUp (0) { softwareVersion = 0 }
        let bytes = build(|w| {
            w.start_struct(&TLVTag::Anonymous).unwrap();
            w.u32(&TLVTag::Context(0), 7).unwrap();
            w.end_container().unwrap();
        });
        let v = event_json(40, 0, &TLVElement::new(&bytes));
        assert_eq!(v, json!({"softwareVersion": 7}));
    }

    #[test]
    fn unknown_event_falls_back_to_tag_based() {
        let bytes = build(|w| {
            w.start_struct(&TLVTag::Anonymous).unwrap();
            w.u32(&TLVTag::Context(0), 7).unwrap();
            w.end_container().unwrap();
        });
        // Cluster known, event id it has never heard of.
        assert_eq!(event_json(40, 9999, &TLVElement::new(&bytes)), json!({"0": 7}));
        // Cluster unknown entirely.
        assert_eq!(event_json(0xFFF1_0001, 0, &TLVElement::new(&bytes)), json!({"0": 7}));
    }

    #[test]
    fn empty_payload_is_null() {
        assert_eq!(event_json(40, 0, &TLVElement::new(&[])), Value::Null);
    }

    #[test]
    fn malformed_payload_is_null_not_a_panic() {
        // 0x15 opens a struct that never closes.
        assert_eq!(event_json(40, 0, &TLVElement::new(&[0x15])), Value::Null);
    }
}
