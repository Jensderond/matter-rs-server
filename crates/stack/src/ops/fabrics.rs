//! The device's own view of the fabrics it belongs to
//! (`OperationalCredentials.Fabrics`), removing one of them, and pushing a new
//! label for *our* fabric.

use matter_rs_controller::stack_api::{
    AttributePathSpec, DeviceFabric, StackError, StackErrorKind,
};
use rs_matter::crypto::Crypto;
use serde_json::Value;

use crate::ctx::{map_err, StackCtx};
use crate::ops::{interact, OP_CREDS, ROOT_ENDPOINT};

/// `OperationalCredentials.Fabrics` attribute id.
const FABRICS_ATTR: u32 = 1;

/// Node's wording, reproduced verbatim: the WS client matches on it.
const NO_FABRICS: &str = "No or invalid response received while querying fabrics";

/// `FabricDescriptorStruct` field ids (IDL: `controller-clusters-V1.6.0.0.matter`,
/// cluster 62). The value arrives tag-based, so these are the JSON keys.
mod tag {
    /// `vendor_id vendorID`
    pub const VENDOR_ID: &str = "2";
    /// `fabric_id fabricID`
    pub const FABRIC_ID: &str = "3";
    /// `char_string<32> label`
    pub const LABEL: &str = "5";
    /// `fabric_idx fabricIndex`
    pub const FABRIC_INDEX: &str = "254";
}

// TODO(task16): remove the allows on the three entry points below — all of them
// are reached only through `StackHandle`.
#[allow(dead_code)]
pub(crate) async fn device_fabrics<C: Crypto>(
    ctx: &StackCtx<C>,
    node_id: u64,
) -> Result<Vec<DeviceFabric>, StackError> {
    // `fabric_filtered = false` on purpose: the point of this command is to list
    // the *other* administrators' fabrics too, and a fabric-filtered read would
    // return only ours.
    let paths = [AttributePathSpec {
        endpoint: Some(ROOT_ENDPOINT),
        cluster: Some(OP_CREDS),
        attribute: Some(FABRICS_ATTR),
    }];
    let pairs = interact::read_attributes(ctx, node_id, &paths, false).await?;
    parse_fabrics(&pairs)
}

/// Tag-based `FabricDescriptorStruct` list -> [`DeviceFabric`]s.
///
/// Pure, because everything that can realistically go wrong here is a shape
/// problem in what the device sent, not a transport failure.
fn parse_fabrics(pairs: &[(String, Value)]) -> Result<Vec<DeviceFabric>, StackError> {
    let key = format!("{ROOT_ENDPOINT}/{OP_CREDS}/{FABRICS_ATTR}");
    // Matched by path rather than taken positionally: a device may report extra
    // paths (or none), and mapping some other attribute's value as a fabric list
    // would be worse than reporting nothing.
    let value = pairs.iter().find(|(k, _)| *k == key).map(|(_, v)| v);

    let Some(Value::Array(entries)) = value else {
        return Err(StackError::new(StackErrorKind::Sdk, NO_FABRICS));
    };
    // A commissioned node always belongs to at least the fabric we are asking
    // over, so an empty list means the read did not really answer.
    if entries.is_empty() {
        return Err(StackError::new(StackErrorKind::Sdk, NO_FABRICS));
    }

    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        let Value::Object(fields) = entry else {
            tracing::warn!("fabrics list entry is not a struct: {entry}");
            continue;
        };
        let num = |t: &str| fields.get(t).and_then(Value::as_u64);
        out.push(DeviceFabric {
            // Truncating rather than rejecting: an out-of-range id from a device
            // is not worth dropping the whole entry (and hence hiding a fabric
            // the user may want to remove) over.
            fabric_id: num(tag::FABRIC_ID).unwrap_or(0),
            vendor_id: num(tag::VENDOR_ID).unwrap_or(0) as u16,
            fabric_index: num(tag::FABRIC_INDEX).unwrap_or(0) as u8,
            fabric_label: fields
                .get(tag::LABEL)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        });
    }

    Ok(out)
}

#[allow(dead_code)]
pub(crate) async fn remove_device_fabric<C: Crypto>(
    ctx: &StackCtx<C>,
    node_id: u64,
    fabric_index: u8,
) -> Result<(), StackError> {
    let resp = interact::invoke(
        ctx,
        node_id,
        ROOT_ENDPOINT,
        OP_CREDS,
        "removeFabric",
        &serde_json::json!({ "fabricIndex": fabric_index }),
        None,
    )
    .await?;
    check_noc_response("RemoveFabric", &resp)
}

/// `NOCResponse.statusCode` must be `OK` (0).
///
/// Split out of the await because this is the whole failure surface of every
/// command that answers `NOCResponse` (`RemoveFabric`, `UpdateFabricLabel`): the
/// IM invoke itself succeeds — the device *did* run the command — and the outcome
/// lives in the payload.
fn check_noc_response(command: &str, resp: &Value) -> Result<(), StackError> {
    // Name-based, because `interact::invoke` converts command responses through
    // the `gen` tables (`response struct NOCResponse { statusCode = 0, ... }`).
    match resp.get("statusCode").and_then(Value::as_u64) {
        Some(0) => Ok(()),
        Some(status) => Err(StackError::new(
            StackErrorKind::Sdk,
            format!("{command} failed with status {status}"),
        )),
        None => Err(StackError::new(
            StackErrorKind::Sdk,
            format!("{command} returned no status code"),
        )),
    }
}

#[allow(dead_code)]
pub(crate) async fn update_fabric_label<C: Crypto>(
    ctx: &StackCtx<C>,
    label: &str,
) -> Result<(), StackError> {
    // Local state first: it is the authoritative copy (rs-matter re-adds the
    // fabric from storage on every start), and the device-side pushes below are
    // only there so other administrators see the new name.
    //
    // Clamped to the byte budget for the same reason as at bootstrap: the stored
    // label is capped at 32 *chars*, `Fabric::label` holds 32 *bytes*, and
    // `update_label` answers `ConstraintError` — not a truncation — for anything
    // longer.
    let label = crate::identity::truncate_to_bytes(label, crate::identity::FABRIC_LABEL_MAX_BYTES);
    ctx.matter
        .with_state(|s| s.fabrics.update_label(ctx.fab_idx, label).map(|_| ()))
        .map_err(map_err)?;

    // Snapshot the node list: the borrow must not be held across an await, and
    // a supervisor starting or stopping mid-loop is not worth serialising.
    let nodes: Vec<u64> = ctx.supervisors.borrow().keys().copied().collect();
    for node_id in nodes {
        push_fabric_label(ctx, node_id, label).await;
    }

    Ok(())
}

/// Tell one node the new label for our fabric. Best-effort by design: an
/// unreachable or objecting node must not fail the operation that triggered this,
/// it just keeps the old label until someone renames again.
///
/// Shared with `ops::commission`, which does the same push at the end of a fresh
/// commissioning — same truncation, same command, same warning.
pub(crate) async fn push_fabric_label<C: Crypto>(ctx: &StackCtx<C>, node_id: u64, label: &str) {
    // `char_string<32>` is 32 *bytes*, while the label is validated at 32
    // *chars*, so a multibyte label has to be clamped here too — `update_label`
    // and the device both reject rather than truncate.
    let label = crate::identity::truncate_to_bytes(label, crate::identity::FABRIC_LABEL_MAX_BYTES);

    // `UpdateFabricLabel` answers `NOCResponse`, so a device that *ran* the
    // command and *refused* it (`kLabelConflict`, say) returns `Ok` from the
    // invoke. Without the status check that rejection produced no log line at all.
    let outcome = interact::invoke(
        ctx,
        node_id,
        ROOT_ENDPOINT,
        OP_CREDS,
        "updateFabricLabel",
        &serde_json::json!({ "label": label }),
        None,
    )
    .await
    .and_then(|resp| check_noc_response("UpdateFabricLabel", &resp));

    if let Err(e) = outcome {
        tracing::warn!("UpdateFabricLabel on node {node_id} failed: {}", e.message);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pairs(value: Value) -> Vec<(String, Value)> {
        vec![("0/62/1".to_string(), value)]
    }

    #[test]
    fn a_full_fabric_descriptor_maps_field_for_field() {
        let out = parse_fabrics(&pairs(json!([{
            "1": "BASE64ROOTKEY",         // rootPublicKey — ignored
            "2": 0xFFF1,                  // vendorID
            "3": 1,                       // fabricID
            "4": 112_233,                 // nodeID (the device's view of us) — ignored
            "5": "HomeAssistant",         // label
            "254": 3,                     // fabricIndex
        }])))
        .expect("valid list");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].vendor_id, 0xFFF1);
        assert_eq!(out[0].fabric_id, 1);
        assert_eq!(out[0].fabric_index, 3);
        assert_eq!(out[0].fabric_label, "HomeAssistant");
    }

    #[test]
    fn several_fabrics_keep_their_order() {
        let out = parse_fabrics(&pairs(json!([
            {"2": 1, "3": 10, "5": "a", "254": 1},
            {"2": 2, "3": 20, "5": "b", "254": 2},
        ])))
        .expect("valid list");
        assert_eq!(
            out.iter().map(|f| f.fabric_index).collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(out[1].fabric_label, "b");
    }

    /// Every field is optional on the wire (`label` is routinely empty, and a
    /// fabric-scoped struct read unfiltered may omit `fabricIndex`), so a missing
    /// one must default rather than drop the entry — the entry is what the user
    /// needs in order to remove that fabric.
    #[test]
    fn missing_fields_default_instead_of_dropping_the_fabric() {
        let out = parse_fabrics(&pairs(json!([{"254": 2}]))).expect("valid list");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].fabric_index, 2);
        assert_eq!(out[0].vendor_id, 0);
        assert_eq!(out[0].fabric_id, 0);
        assert_eq!(out[0].fabric_label, "");
    }

    #[test]
    fn a_non_struct_entry_is_skipped_not_fatal() {
        let out = parse_fabrics(&pairs(json!([7, {"254": 1}]))).expect("valid list");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].fabric_index, 1);
    }

    /// The exact string a WS client matches on.
    #[test]
    fn empty_missing_and_non_array_reads_all_report_the_node_message() {
        for pairs in [
            pairs(json!([])),
            pairs(json!(null)),
            pairs(json!(7)),
            pairs(json!({"254": 1})),
            // Right value, wrong path: must not be mistaken for the fabrics list.
            vec![("0/62/0".to_string(), json!([{"254": 1}]))],
            vec![],
        ] {
            let e = parse_fabrics(&pairs).expect_err("must not be accepted");
            assert_eq!(e.kind, StackErrorKind::Sdk);
            assert_eq!(e.message, NO_FABRICS);
        }
    }

    #[test]
    fn noc_status_zero_is_success() {
        assert!(check_noc_response("RemoveFabric", &json!({"statusCode": 0})).is_ok());
        // `fabricIndex`/`debugText` are optional and irrelevant to the outcome.
        assert!(check_noc_response(
            "RemoveFabric",
            &json!({"statusCode": 0, "fabricIndex": 2, "debugText": "ok"})
        )
        .is_ok());
    }

    #[test]
    fn a_nonzero_noc_status_names_the_code() {
        // 11 = kInvalidFabricIndex, the status a wrong index actually returns.
        let e = check_noc_response("RemoveFabric", &json!({"statusCode": 11}))
            .expect_err("non-zero status must fail");
        assert_eq!(e.kind, StackErrorKind::Sdk);
        assert_eq!(e.message, "RemoveFabric failed with status 11");
    }

    /// A device that answers the invoke but not with an `NOCResponse` must not
    /// read as success — the fabric may still be there.
    #[test]
    fn a_malformed_noc_response_is_not_success() {
        for resp in [
            json!(null),
            json!({}),
            json!({"statusCode": "0"}),
            json!({"statusCode": null}),
            json!({"0": 0}), // tag-based: the name-based conversion did not happen
            json!(7),
        ] {
            let e = check_noc_response("RemoveFabric", &resp)
                .expect_err("must not be accepted as success");
            assert_eq!(e.message, "RemoveFabric returned no status code", "for {resp}");
        }
    }
}
