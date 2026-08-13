//! Generic IM operations. Each function initiates a CASE exchange (cheap on a
//! warm session; internally mDNS-resolves on a cold one) and drives the
//! rs-matter sender state machines by hand, because the closure-based
//! `ImClient::read_with` / `invoke_with` helpers hide the response chunk the
//! multi-chunk loops need.
//!
//! Two borrow rules shape all of it: a response borrows the exchange RX buffer
//! and cannot escape, so TLV is converted to owned JSON inside the borrow; and
//! the request-build step is re-run on every MRP retransmit, so it must stay a
//! pure function of data prepared beforehand.

use std::collections::BTreeMap;

use matter_rs_controller::stack_api::{AttributePathSpec, StackError, StackErrorKind};
use matter_rs_gen::{Cluster, Cmd};
use rs_matter::crypto::Crypto;
use rs_matter::error::{Error, ErrorCode};
use rs_matter::im::client::{ImClient, TxOutcome};
use rs_matter::im::{AttrDataTag, AttrPath, CmdDataTag, CmdResp, GenericPath, IMStatusCode};
use rs_matter::tlv::{TLVElement, TLVTag, TLVWrite, ToTLV};
use rs_matter::transport::exchange::{Exchange, MAX_EXCHANGE_TX_BUF_SIZE};
use rs_matter::utils::storage::WriteBuf;
use serde_json::{Map, Value};

use crate::ctx::{with_timeout, StackCtx, IM_TIMEOUT_SECS, INTERVIEW_TIMEOUT_SECS};
use crate::reports::AttrAccumulator;
use crate::tlv_json::{self, TypeHint};

/// Devices reject spec-timed commands sent untimed, and the WS API has no way to
/// ask for a budget, so timed commands get one by default. Node uses the same
/// 10s figure.
const DEFAULT_TIMED_MS: u16 = 10_000;

/// The timed-invoke budget actually to use.
///
/// A caller-supplied `Some(0)` is dropped: zero milliseconds has already expired
/// by the time the device parses the TimedRequest, so honouring it would send a
/// request guaranteed to fail while claiming to be timed.
fn normalize_timed(timed_ms: Option<u16>, cmd: &Cmd) -> Option<u16> {
    timed_ms
        .filter(|ms| *ms > 0)
        .or(if cmd.is_timed { Some(DEFAULT_TIMED_MS) } else { None })
}

fn to_attr_path(p: &AttributePathSpec) -> AttrPath {
    // An all-`None` spec is the wildcard the interview relies on: `AttrPath`
    // encodes as an empty TLV list, which the spec reads as "every attribute on
    // every cluster on every endpoint".
    AttrPath::from_gp(&GenericPath::new(p.endpoint, p.cluster, p.attribute))
}

/// TLV-encode `f`'s output standalone, under an anonymous tag.
///
/// JSON->TLV happens here, *before* the exchange opens, for two reasons. The
/// build closure the senders take is `FnMut` and re-run on every retransmit, so
/// encoding inside it repeats the work; and an encoding failure raised in there
/// arrives at the caller as an indistinguishable rs-matter `Error`, leaving no
/// way to tell "the client sent us a bad payload" from "the device misbehaved"
/// other than pattern-matching a formatted message. Encoded up front, the two
/// are separate code paths. The build closure then only re-tags the finished
/// bytes, which is the idiom rs-matter's own write test uses
/// (`rs-matter-ref/rs-matter/tests/im/client_writes.rs:65`).
///
/// The separation covers *shape* errors — unknown field names, wrong JSON types,
/// out-of-range numbers — and not size. The buffer is a whole-message budget
/// (`MAX_EXCHANGE_TX_BUF_SIZE`, `rs-matter-ref/rs-matter/src/transport/exchange.rs:64`),
/// so a payload within a few dozen bytes of it passes here and then fails as
/// `NoSpace` from inside the exchange, classified `Sdk`. Deliberately not
/// tightened: subtracting a guessed wrapper size would reject payloads the
/// transport would have accepted, which is the worse failure.
fn encode_anonymous<F>(f: F) -> Result<Vec<u8>, Error>
where
    F: FnOnce(&mut WriteBuf<'_>) -> Result<(), Error>,
{
    let mut buf = vec![0u8; MAX_EXCHANGE_TX_BUF_SIZE];
    let mut wb = WriteBuf::new(&mut buf);
    f(&mut wb)?;
    let encoded = wb.as_slice();
    if encoded.is_empty() {
        // `ToTLV for TLVElement` special-cases an empty element to write
        // *nothing* (`rs-matter-ref/rs-matter/src/tlv/traits.rs:149`), so
        // re-tagging zero bytes would emit a request with the `Data` field
        // silently missing rather than an error.
        return Err(ErrorCode::InvalidData.into());
    }
    Ok(encoded.to_vec())
}

pub(crate) async fn read_attributes<C: Crypto>(
    ctx: &StackCtx<C>,
    node_id: u64,
    paths: &[AttributePathSpec],
    fabric_filtered: bool,
) -> Result<Vec<(String, Value)>, StackError> {
    read_attributes_inner(ctx, node_id, paths, fabric_filtered, IM_TIMEOUT_SECS).await
}

async fn read_attributes_inner<C: Crypto>(
    ctx: &StackCtx<C>,
    node_id: u64,
    paths: &[AttributePathSpec],
    fabric_filtered: bool,
    timeout_secs: u64,
) -> Result<Vec<(String, Value)>, StackError> {
    let attr_paths: Vec<AttrPath> = paths.iter().map(to_attr_path).collect();

    with_timeout(timeout_secs, async {
        let exchange = Exchange::initiate(ctx.matter, &ctx.crypto, ctx.fab_idx, node_id).await?;
        let mut sender = exchange.read_sender().await?;

        let mut chunk = loop {
            match sender.tx().await? {
                TxOutcome::BuildRequest(builder) => {
                    sender = builder
                        .attr_requests_from(&attr_paths)?
                        .fabric_filtered(fabric_filtered)?
                        .end()?;
                }
                TxOutcome::GotResponse(c) => break c,
            }
        };

        // One accumulator across every chunk of this response: `complete()`
        // walks the chunks of a single logical ReportData, which is exactly the
        // span a chunked list attribute can be split over.
        let who = format!("read node {node_id}");
        let mut acc = AttrAccumulator::default();
        loop {
            {
                let resp = chunk.response()?;
                acc.absorb(&resp, &who);
            }
            match chunk.complete().await? {
                Some(next) => chunk = next,
                None => break,
            }
        }
        if acc.failures() > 0 {
            // Partial results beat none — a single unconvertible attribute must
            // not discard a 120s wildcard interview — but the caller's data is
            // incomplete and that has to be visible.
            tracing::warn!("{who}: {} attribute report(s) skipped", acc.failures());
        }

        Ok(acc.into_pairs())
    })
    .await
}

// TODO(task16): remove the allow — `interview` and `write_attribute` are
// reached only through `StackHandle`; every other operation here already has an
// in-crate caller.
#[allow(dead_code)]
pub(crate) async fn interview<C: Crypto>(
    ctx: &StackCtx<C>,
    node_id: u64,
) -> Result<BTreeMap<String, Value>, StackError> {
    // Full wildcard, fabric-filtered (Node interview behaviour), on the bigger
    // budget: a bridge's whole attribute tree takes many chunks.
    let all = [AttributePathSpec { endpoint: None, cluster: None, attribute: None }];
    read_attributes_inner(ctx, node_id, &all, true, INTERVIEW_TIMEOUT_SECS)
        .await
        .map(|v| v.into_iter().collect())
}

#[allow(dead_code)] // TODO(task16): see `interview` above.
pub(crate) async fn write_attribute<C: Crypto>(
    ctx: &StackCtx<C>,
    node_id: u64,
    endpoint: u16,
    cluster: u32,
    attribute: u32,
    value: &Value,
) -> Result<u8, StackError> {
    let meta = matter_rs_gen::cluster(cluster);
    // `TypeHint` requires `cluster` whenever `ty` names a struct; `meta` is the
    // cluster the attribute was just looked up on, so it always resolves.
    let hint = meta
        .and_then(|cl| cl.attr(attribute))
        .map(|a| TypeHint { ty: a.ty, is_list: a.is_list, cluster: meta });

    let encoded = encode_anonymous(|w| tlv_json::write_json(w, &TLVTag::Anonymous, value, hint))
        .map_err(|e| {
            StackError::new(
                StackErrorKind::InvalidArguments,
                format!("Invalid value for attribute {endpoint}/{cluster}/{attribute}: Error::{e}"),
            )
        })?;

    with_timeout(IM_TIMEOUT_SECS, async {
        let exchange = Exchange::initiate(ctx.matter, &ctx.crypto, ctx.fab_idx, node_id).await?;
        let mut sender = exchange.write_sender(None).await?;

        let handle = loop {
            match sender.tx().await? {
                TxOutcome::BuildRequest(builder) => {
                    let entry = builder
                        .write_requests()?
                        .push()?
                        .path(endpoint, cluster, attribute)?
                        .data(|w| {
                            TLVElement::new(&encoded)
                                .to_tlv(&TLVTag::Context(AttrDataTag::Data as u8), w)
                        })?
                        .end()?;
                    sender = entry.end()?.end()?;
                }
                TxOutcome::GotResponse(h) => break h,
            }
        };

        let resp = handle.response()?;
        // Only statuses for the path we actually wrote count. A device may echo a
        // stale or expanded path, and treating one of those as ours would report
        // an unrelated success. Same matching as rs-matter's own
        // `WriteResp::statuses` (`im/encoding/attr/write.rs:143`), plus the
        // endpoint — see the report for why the raw code is kept instead.
        let mut ours: Option<u8> = None;
        for s in resp.write_responses.iter() {
            let s = s?;
            if s.path.endpoint != Some(endpoint)
                || s.path.cluster != Some(cluster)
                || s.path.attr != Some(attribute)
            {
                tracing::debug!("write: ignoring status for unrelated path {:?}", s.path);
                continue;
            }
            let code = s.status.status as u8;
            // A list write can be answered per element; the first failure is the
            // useful one, and a later success must not mask it.
            if code != 0 {
                ours = Some(code);
                break;
            }
            ours = Some(0);
        }

        // No status at all is not success: an empty `WriteResponses` array, or one
        // that never mentions our path, means we have no idea what the device did.
        Ok(ours.ok_or_else(|| {
            StackError::new(
                StackErrorKind::Sdk,
                format!(
                    "device returned no write status for {endpoint}/{cluster}/{attribute}"
                ),
            )
        }))
    })
    .await?
}

pub(crate) async fn invoke<C: Crypto>(
    ctx: &StackCtx<C>,
    node_id: u64,
    endpoint: u16,
    cluster: u32,
    command_name: &str,
    payload: &Value,
    timed_ms: Option<u16>,
) -> Result<Value, StackError> {
    let meta = matter_rs_gen::cluster(cluster).ok_or_else(|| {
        StackError::new(StackErrorKind::InvalidArguments, format!("Cluster Id \"{cluster}\" unknown"))
    })?;
    let cmd = meta.find_command_ci(command_name).ok_or_else(|| {
        StackError::new(
            StackErrorKind::InvalidArguments,
            format!("Command \"{command_name}\" does not exist on cluster \"{}\"", meta.name),
        )
    })?;
    let timed_ms = normalize_timed(timed_ms, cmd);

    let args = encode_command_args(meta, cmd, payload).map_err(|e| {
        StackError::new(
            StackErrorKind::InvalidArguments,
            format!("Invalid payload for command \"{command_name}\": Error::{e}"),
        )
    })?;
    let output = cmd.output.and_then(|o| meta.find_struct(o));

    // The inner `Result` carries a device-reported command failure: it needs a
    // `StackError` of its own (an IM status is richer than any rs-matter
    // `ErrorCode` it could be laundered through), so it travels as the success
    // value of the timeout future and is unwrapped by the trailing `?`.
    with_timeout(IM_TIMEOUT_SECS, async {
        let exchange = Exchange::initiate(ctx.matter, &ctx.crypto, ctx.fab_idx, node_id).await?;
        let mut sender = exchange.invoke_sender(timed_ms).await?;

        let mut chunk = loop {
            match sender.tx().await? {
                TxOutcome::BuildRequest(builder) => {
                    sender = builder
                        .suppress_response(false)?
                        .timed_request(timed_ms.is_some())?
                        .invoke_requests()?
                        .push()?
                        .path(endpoint, cluster, cmd.code)?
                        .data(|w| {
                            TLVElement::new(&args)
                                .to_tlv(&TLVTag::Context(CmdDataTag::Data as u8), w)
                        })?
                        .end()? // close CmdData
                        .end()? // close InvokeRequests array
                        .end()?; // close InvokeRequestMessage
                }
                TxOutcome::GotResponse(c) => break c,
            }
        };

        // DefaultSuccess commands answer with a bare StatusResponse, for which
        // `response()` returns `None` after having already validated it as
        // Success (`rs-matter-ref/rs-matter/src/im/client.rs:944,978`) — so Null
        // is the result and no explicit status-only branch is needed.
        let mut result: Result<Value, StackError> = Ok(Value::Null);
        let mut got_payload = false;
        let mut seen = 0usize;
        loop {
            if let Some(resp) = chunk.response()? {
                if let Some(responses) = &resp.invoke_responses {
                    for r in responses.iter() {
                        seen += 1;
                        match r? {
                            CmdResp::Cmd(data) => {
                                let json = match output {
                                    Some(out) => {
                                        tlv_json::tlv_to_json_named(&data.data, out.fields, meta)?
                                    }
                                    None => tlv_json::tlv_to_json(&data.data)?,
                                };
                                // A failure already reported for this command
                                // outranks any payload, and the first payload
                                // wins over later ones.
                                if result.is_ok() && !got_payload {
                                    got_payload = true;
                                    result = Ok(json);
                                }
                            }
                            CmdResp::Status(s) => {
                                if s.status.status != IMStatusCode::Success {
                                    result = Err(command_failed(command_name, s.status.status));
                                }
                            }
                        }
                    }
                }
            }
            match chunk.complete().await? {
                Some(next) => chunk = next,
                None => break,
            }
        }
        if seen > 1 {
            // We send exactly one CmdData, so the spec allows exactly one
            // response. More means the device is batching something we did not
            // ask for, and only one of them can be returned.
            tracing::warn!(
                "invoke \"{command_name}\" on {endpoint}/{cluster}: device returned {seen} \
                 responses for one command; keeping the first payload and any failure"
            );
        }

        Ok(result)
    })
    .await?
}

/// A command's arguments as a TLV struct, anonymously tagged.
///
/// Commands with no request struct still take an empty args struct: rs-matter
/// and the C++ SDK both require the `CommandFields` element to be present.
fn encode_command_args(
    meta: &'static Cluster,
    cmd: &Cmd,
    payload: &Value,
) -> Result<Vec<u8>, Error> {
    let empty = Map::new();
    let obj = match payload {
        Value::Null => &empty,
        Value::Object(o) => o,
        // A non-object payload cannot name fields, so it is a caller error
        // rather than something to encode heuristically.
        _ => return Err(ErrorCode::InvalidData.into()),
    };
    let fields = cmd.input.and_then(|s| meta.find_struct(s)).map(|s| s.fields);

    encode_anonymous(|w| match fields {
        Some(fields) => tlv_json::write_json_named(w, &TLVTag::Anonymous, obj, fields, meta),
        None => {
            w.start_struct(&TLVTag::Anonymous)?;
            w.end_container()
        }
    })
}

/// A device answering an invoke with a non-Success status.
///
/// Deliberately not routed through `IMStatusCode::to_error_code`: that maps
/// `NotFound` onto `ErrorCode::NotFound`, which `map_err` reads as "mDNS could
/// not resolve the node" — the opposite of what a per-command NotFound means.
fn command_failed(command_name: &str, status: IMStatusCode) -> StackError {
    let kind = match status {
        IMStatusCode::Busy => StackErrorKind::Busy,
        IMStatusCode::Timeout => StackErrorKind::Timeout,
        _ => StackErrorKind::Sdk,
    };
    StackError::new(
        kind,
        format!(
            "Command \"{command_name}\" failed with IM status {status:?} (0x{:02x})",
            status as u8
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn wildcard_spec_becomes_an_empty_path() {
        let p = to_attr_path(&AttributePathSpec { endpoint: None, cluster: None, attribute: None });
        // What `interview` sends. `AttrPath::default()` is all-`None`, which is
        // the on-wire empty list.
        assert_eq!(p, AttrPath::default());
    }

    #[test]
    fn concrete_spec_maps_segment_for_segment() {
        let p = to_attr_path(&AttributePathSpec {
            endpoint: Some(1),
            cluster: Some(6),
            attribute: Some(0),
        });
        assert_eq!((p.endpoint, p.cluster, p.attr), (Some(1), Some(6), Some(0)));
        // Never set: a node id in the path is for proxies, and list_index would
        // turn the read into a single-element read.
        assert!(p.node.is_none());
        assert!(p.list_index.is_none());
    }

    #[test]
    fn partial_wildcard_keeps_the_concrete_segments() {
        let p = to_attr_path(&AttributePathSpec {
            endpoint: None,
            cluster: Some(29),
            attribute: None,
        });
        assert_eq!((p.endpoint, p.cluster, p.attr), (None, Some(29), None));
    }

    #[test]
    fn command_args_are_named_and_ordered() {
        let meta = matter_rs_gen::cluster(8).expect("LevelControl");
        let cmd = meta.find_command_ci("moveToLevel").expect("MoveToLevel");
        let bytes = encode_command_args(
            meta,
            cmd,
            &json!({"transitionTime": 3, "level": 254, "optionsMask": 0, "optionsOverride": 0}),
        )
        .expect("valid payload");

        // Wire order, not just contents: TLV struct members are canonically
        // ordered by ascending context tag, and comparing two `Value::Object`s
        // cannot see order at all. Read the tags off the encoded container.
        let elem = TLVElement::new(&bytes);
        let tags: Vec<u8> = elem
            .container()
            .expect("struct")
            .iter()
            .map(|c| match c.expect("member").tag().expect("tag") {
                TLVTag::Context(n) => n,
                other => unreachable!("unexpected member tag {other:?}"),
            })
            .collect();
        // level=0, transitionTime=1, optionsMask=2, optionsOverride=3 — emitted
        // ascending even though the JSON listed transitionTime first.
        assert_eq!(tags, [0, 1, 2, 3]);

        let v = tlv_json::tlv_to_json(&elem).expect("parse");
        assert_eq!(v, json!({"0": 254, "1": 3, "2": 0, "3": 0}));
    }

    #[test]
    fn command_without_input_gets_an_empty_struct() {
        let meta = matter_rs_gen::cluster(6).expect("OnOff");
        let cmd = meta.find_command_ci("toggle").expect("Toggle");
        let bytes = encode_command_args(meta, cmd, &Value::Null).expect("no args");
        let v = tlv_json::tlv_to_json(&TLVElement::new(&bytes)).expect("parse");
        assert_eq!(v, json!({}));
    }

    #[test]
    fn unknown_field_and_non_object_payloads_are_rejected() {
        let meta = matter_rs_gen::cluster(8).expect("LevelControl");
        let cmd = meta.find_command_ci("moveToLevel").expect("MoveToLevel");
        // This is the failure that must reach the client as InvalidArguments,
        // and it is detected here rather than mid-exchange.
        assert!(encode_command_args(meta, cmd, &json!({"nope": 1})).is_err());
        assert!(encode_command_args(meta, cmd, &json!(7)).is_err());
    }

    /// The pre-encode design hinges on re-tagging a finished TLV element into
    /// the request builder. Containers make that non-obvious (the re-tag has to
    /// carry every nested element and the matching end-of-container), so pin it.
    #[test]
    fn retagging_a_prebuilt_struct_preserves_nesting() {
        let inner = encode_anonymous(|w| {
            w.start_struct(&TLVTag::Anonymous)?;
            w.u16(&TLVTag::Context(0), 4242)?;
            w.start_array(&TLVTag::Context(1))?;
            w.utf8(&TLVTag::Anonymous, "a")?;
            w.utf8(&TLVTag::Anonymous, "b")?;
            w.end_container()?;
            w.end_container()
        })
        .expect("build");

        let retag = || {
            encode_anonymous(|w| {
                w.start_struct(&TLVTag::Anonymous)?;
                TLVElement::new(&inner).to_tlv(&TLVTag::Context(CmdDataTag::Data as u8), &mut *w)?;
                w.end_container()
            })
            .expect("retag")
        };
        let retagged = retag();

        let v = tlv_json::tlv_to_json(&TLVElement::new(&retagged)).expect("parse");
        assert_eq!(v, json!({"1": {"0": 4242, "1": ["a", "b"]}}));

        // The build closure is `FnMut` and re-run on every MRP retransmit, so
        // re-tagging the same element twice must produce identical bytes.
        assert_eq!(retagged, retag());
    }

    #[test]
    fn a_zero_timed_budget_is_treated_as_absent() {
        // Mirrors the `timed_ms` normalisation in `invoke`: 0 would be expired on
        // arrival, so it must not turn into `timed_request(true)`.
        let untimed = matter_rs_gen::cluster(6)
            .and_then(|c| c.find_command_ci("toggle"))
            .expect("OnOff/Toggle");
        assert!(!untimed.is_timed);
        assert_eq!(normalize_timed(Some(0), untimed), None);
        assert_eq!(normalize_timed(None, untimed), None);
        assert_eq!(normalize_timed(Some(5_000), untimed), Some(5_000));

        let timed = matter_rs_gen::cluster(60)
            .and_then(|c| c.find_command_ci("openCommissioningWindow"))
            .expect("AdministratorCommissioning/OpenCommissioningWindow");
        assert!(timed.is_timed);
        // A spec-timed command gets the default rather than being sent untimed.
        assert_eq!(normalize_timed(None, timed), Some(DEFAULT_TIMED_MS));
        assert_eq!(normalize_timed(Some(0), timed), Some(DEFAULT_TIMED_MS));
        assert_eq!(normalize_timed(Some(1_000), timed), Some(1_000));
    }
}
