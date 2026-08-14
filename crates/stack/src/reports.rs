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
use crate::tlv_json::{self, MATTER_EPOCH_OFFSET_US};

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
/// not fit the remaining TX space, devices — rs-matter included, see
/// `send_array_items` at `rs-matter-ref/rs-matter/src/im.rs:2123` — split it into
/// N+1 `AttributeReportIB`s that all carry the *same* endpoint/cluster/attribute:
/// first an empty array tagged `ListIndex = Null`, then one report per element
/// tagged `ListIndex = Some(i)`. Keyed on the path alone they collapse
/// last-wins, and a 40-entry `PartsList` arrives at the client as a bare integer.
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
            match list_index(&data.path) {
                None => self.put(key, json),
                Some(i) => {
                    self.saw_list_chunks = true;
                    self.append(key, i, json, who);
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
    /// preceding `ListIndex = Null` report.
    fn append(&mut self, key: String, i: u16, json: Value, who: &str) {
        let Some(&at) = self.index.get(&key) else {
            // No array to append to. Either the report is malformed, or — the
            // real case — this is a subscription report whose list started in a
            // previous `ReportData` *message*, which arrives as a separate
            // `handle_report` call. Cross-message merging is not implemented, so
            // say so loudly instead of handing the client a truncated array.
            tracing::warn!(
                "{who}: list element {i} for {key} has no array to append to; the list is \
                 split across ReportData messages and is reported incomplete"
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
            Value::Array(arr) => {
                // Indices run 0,1,2,… (`im.rs:2148`); a gap means an element was
                // lost somewhere between the device and here.
                if usize::from(i) != arr.len() {
                    tracing::warn!(
                        "{who}: list index gap on {}: expected {}, got {i}",
                        slot.0,
                        arr.len()
                    );
                }
                arr.push(json);
            }
            other => {
                tracing::warn!(
                    "{who}: list element {i} for {} arrived after a non-array value; restarting \
                     the array",
                    slot.0
                );
                *other = Value::Array(vec![json]);
            }
        }
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

/// `Some(i)` for a chunked-list element, `None` for a whole value.
///
/// `AttrPath::list_index` is `Option<Nullable<u16>>`
/// (`im/encoding/attr.rs:72`), so there are three states and only two meanings:
/// field absent (an unchunked attribute) and field present-but-Null (the empty
/// array that opens a chunked list) both mean "the whole value".
fn list_index(path: &AttrPath) -> Option<u16> {
    path.list_index.as_ref().and_then(|n| n.as_opt_ref()).copied()
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
/// NOTE: the epoch variant is treated as `epoch-us` (microseconds since the
/// Matter epoch, 2000-01-01) because that is how matter.js — and therefore the
/// Node server whose output this has to match — reads `EventDataIB` tag 3.
/// rs-matter's own doc comment on `EventDataTimestamp::EpochTimestamp`
/// (`rs-matter-ref/rs-matter/src/im/encoding/event.rs:349`) instead calls it
/// "Posix milliseconds"; rs-matter only passes the raw `u64` through, so the two
/// readings differ by a factor of 1000 plus the 30-year epoch offset. Task 19's
/// device run is what settles it.
///
/// Delta-encoded timestamps are relative to the previous event on the same
/// subscription, which we do not retain across chunks, so they fall back to the
/// local clock and say so via type 2.
fn convert_timestamp(ts: &EventDataTimestamp) -> (i64, u8) {
    match ts {
        EventDataTimestamp::EpochTimestamp(us) => {
            // The divide keeps this far below `i64::MAX` for every input, so both
            // guards are unreachable — kept because a panicking `+` or `as` here
            // would be a network-triggerable abort if the arithmetic ever changes.
            let unix_ms = (us / 1000).saturating_add(MATTER_EPOCH_OFFSET_US / 1000);
            (i64::try_from(unix_ms).unwrap_or(i64::MAX), 1)
        }
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
    /// collapse to the last one.
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
                // ListIndex = Null opens the array.
                AttrResp::Data(AttrData::new(
                    None,
                    path(0, 29, 3, Some(None)),
                    TLVElement::new(&empty),
                )),
                AttrResp::Data(AttrData::new(
                    None,
                    path(0, 29, 3, Some(Some(0))),
                    TLVElement::new(&e0),
                )),
                AttrResp::Data(AttrData::new(
                    None,
                    path(0, 29, 3, Some(Some(1))),
                    TLVElement::new(&e1),
                )),
                AttrResp::Data(AttrData::new(
                    None,
                    path(0, 29, 3, Some(Some(2))),
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

    #[test]
    fn list_element_without_an_opening_array_is_flagged_not_hidden() {
        let e0 = build(|w| w.u16(&TLVTag::Anonymous, 11).unwrap());
        let bytes = report_bytes(
            &[AttrResp::Data(AttrData::new(
                None,
                path(0, 29, 3, Some(Some(4))),
                TLVElement::new(&e0),
            ))],
            true,
        );
        let acc = absorb(&bytes);
        assert_eq!(acc.orphan_appends(), ["0/29/3"]);
        // The element still reaches the client rather than vanishing.
        assert_eq!(acc.into_pairs(), vec![("0/29/3".to_string(), json!([11]))]);
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
                AttrResp::Data(AttrData::new(
                    None,
                    path(0, 29, 3, Some(None)),
                    TLVElement::new(&empty),
                )),
                AttrResp::Data(AttrData::new(
                    None,
                    path(0, 29, 3, Some(Some(0))),
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

    #[test]
    fn epoch_timestamp_becomes_unix_millis() {
        // 2024-01-01T00:00:00Z = 1_704_067_200_000 unix ms
        //                      =   757_382_400_000_000 us since the Matter epoch
        let (ms, ty) = convert_timestamp(&EventDataTimestamp::EpochTimestamp(757_382_400_000_000));
        assert_eq!((ms, ty), (1_704_067_200_000, 1));
    }

    /// The epoch branch's overflow guards are unreachable — dividing by 1000
    /// brings even `u64::MAX` three orders of magnitude below `i64::MAX` — so
    /// this pins the arithmetic rather than a saturation that cannot happen.
    #[test]
    fn largest_epoch_timestamp_stays_positive_and_exact() {
        let (ms, ty) = convert_timestamp(&EventDataTimestamp::EpochTimestamp(u64::MAX));
        let expected = i64::try_from(u64::MAX / 1000 + MATTER_EPOCH_OFFSET_US / 1000).unwrap();
        assert_eq!((ms, ty), (expected, 1));
    }

    #[test]
    fn system_timestamp_passes_through_as_type_zero() {
        let (ms, ty) = convert_timestamp(&EventDataTimestamp::SystemTimestamp(90_000));
        assert_eq!((ms, ty), (90_000, 0));
    }

    /// Unlike the epoch branch, the system branch forwards the raw `u64`, so the
    /// `i64` clamp is genuinely reachable and must not produce a negative
    /// timestamp.
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
