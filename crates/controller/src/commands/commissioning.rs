//! Commissioning and discovery command family: PASE commissioning over a
//! pairing code or on-network filter, opening a commissioning window, and
//! mDNS discovery of commissionable devices.

use serde_json::{json, Map, Value};

use matter_rs_wire::error::ServerErrorCode;
use matter_rs_wire::node::CommissionableNodeData;

use crate::addr::split_ip_port;
use crate::api::CommandError;
use crate::commands::{err, invalid, narrow, opt_str, opt_u64, require_u64, stack_err};
use crate::real::MatterController;
use crate::stack_api::{CommissionRequest, PaseTarget};
use crate::storage::{allocate_node_id, format_node_date, NodeRecord};

/// A failed commission or its follow-up interview, as the client sees it.
///
/// Routed through `stack_err` rather than pinned to `NodeCommissionFailed` (1):
/// `ops::commission` deliberately classifies a mistyped pairing code and an
/// unparseable `ip_addr` as `InvalidArguments`, and flattening every kind to 1
/// threw that away — while an *empty* `code` in the same command already answers
/// 8 (`commission_with_code` below). So: `InvalidArguments -> 8`,
/// `NodeUnreachable -> 4`, everything else -> 1. The `"Commission failed: "`
/// prefix is kept on every kind, because that is the string HA surfaces.
///
/// UNVERIFIED against the Node server: `matterjs-server` is not checked out on
/// this machine, so which code *it* answers for a malformed pairing code is
/// unknown, and this choice is made on internal consistency alone. If you have
/// the Node source, check `commission_with_code`'s error path there before
/// "fixing" this either way.
fn commission_failed(e: crate::stack_api::StackError) -> CommandError {
    let mut mapped = stack_err(ServerErrorCode::NodeCommissionFailed, e);
    mapped.details = format!("Commission failed: {}", mapped.details);
    mapped
}

/// Shared commission -> interview -> persist -> supervise flow, used by both
/// `commission_with_code` and `commission_on_network`. Holds `alloc_lock` for
/// the whole flow: it serializes node-id allocation the same way Node's
/// mutex does, and PASE commissioning serializes upstream anyway.
async fn do_commission(c: &MatterController, target: PaseTarget) -> Result<Value, CommandError> {
    let _guard = c.alloc_lock.lock().await;

    // Allocate + persist the node id BEFORE commissioning. `update_config`
    // serializes the whole read-modify-write itself, so `alloc_lock` is no longer
    // what keeps two allocations apart — it still is what keeps two PASE flows
    // apart. A persistence failure is logged inside `update_config` and does not
    // abort the flow: the id is reserved in memory for the rest of this run
    // (`registry.contains` sees it once the node is inserted), and refusing to
    // commission because config.json could not be rewritten would be a worse
    // trade on a homelab disk that just filled up.
    let (node_id, _persisted) = c.update_config(|cfg| {
        allocate_node_id(cfg, |id| c.registry.contains(id) || id == c.identity.controller_node_id)
    }).await;
    let fabric_label = c.config_snapshot().fabric_label;

    let req = CommissionRequest { node_id, target, fabric_label };
    let outcome = c.stack.commission(req).await.map_err(commission_failed)?;

    let attributes = match c.stack.interview(node_id).await {
        Ok(a) => a,
        Err(e) => {
            let _ = c.stack.remove_device_fabric(node_id, outcome.device_fabric_index).await;
            return Err(commission_failed(e));
        }
    };

    let now = format_node_date(std::time::SystemTime::now());
    let (ip, _port) = split_ip_port(&outcome.address);
    let record = NodeRecord {
        node_id,
        date_commissioned: now.clone(),
        last_interview: now,
        device_fabric_index: outcome.device_fabric_index,
        addresses: vec![ip],
        attributes: attributes.into_iter().collect(),
    };
    c.registry.insert(record.clone());
    if let Err(e) = c.storage.save_node(&record) {
        tracing::error!("persist node {node_id}: {e}");
    }

    let node_data = c.registry.node_data(node_id).ok_or_else(|| {
        err(ServerErrorCode::SdkStackError, "node vanished immediately after commissioning (internal error)")
    })?;
    let value = serde_json::to_value(&node_data).unwrap();
    let _ = c.events.send(matter_rs_wire::envelope::EventMessage {
        event: "node_added".into(),
        data: value.clone(),
    });
    c.stack.start_supervisor(node_id).await;
    Ok(value)
}

pub async fn commission_with_code(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let code = opt_str(args, "code").unwrap_or("");
    if code.is_empty() {
        return Err(invalid("No pairing code provided"));
    }
    // `network_only` is accepted-and-ignored: we have no BLE, always on-network.
    do_commission(c, PaseTarget::Code { code: code.to_string() }).await
}

pub async fn commission_on_network(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let passcode: u32 = narrow(
        opt_u64(args, "setup_pin_code").ok_or_else(|| invalid("No passcode provided"))?,
        "setup_pin_code",
    )?;
    let filter_type = opt_u64(args, "filter_type");
    let filter = opt_u64(args, "filter");

    // Each filter is narrowed to the width mDNS actually advertises it in, so an
    // out-of-range value is rejected instead of silently discovering a different
    // device (`filter: 256` as a short discriminator used to mean 0).
    let (mut short_discriminator, mut long_discriminator, mut vendor_id) = (None, None, None);
    match filter_type {
        Some(1) => {
            let f = filter.ok_or_else(|| invalid("filter required for filter_type 1 (short discriminator)"))?;
            short_discriminator = Some(narrow::<u8>(f, "filter (short discriminator)")?);
        }
        Some(2) => {
            let f = filter.ok_or_else(|| invalid("filter required for filter_type 2 (long discriminator)"))?;
            long_discriminator = Some(narrow::<u16>(f, "filter (long discriminator)")?);
        }
        Some(3) => {
            let f = filter.ok_or_else(|| invalid("filter required for filter_type 3 (vendor ID)"))?;
            vendor_id = Some(narrow::<u16>(f, "filter (vendor ID)")?);
        }
        _ => {}
    }

    let target = match opt_str(args, "ip_addr").filter(|ip| !ip.starts_with("fe80")) {
        Some(ip_addr) => PaseTarget::Address { passcode, addr: format!("{ip_addr}:5540") },
        None => PaseTarget::OnNetwork { passcode, long_discriminator, short_discriminator, vendor_id },
    };
    do_commission(c, target).await
}

pub async fn open_commissioning_window(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    // iteration/option/discriminator accepted-and-ignored (Node behavior).
    // 65536 used to truncate to 0, i.e. a window closing the instant it opened.
    let timeout: u16 = narrow(opt_u64(args, "timeout").unwrap_or(300), "timeout")?;
    let info = c
        .stack
        .open_commissioning_window(node_id, timeout)
        .await
        .map_err(|e| stack_err(ServerErrorCode::SdkStackError, e))?;
    Ok(json!({
        "setup_pin_code": info.setup_pin_code,
        "setup_manual_code": info.setup_manual_code,
        "setup_qr_code": info.setup_qr_code,
    }))
}

pub async fn discover(c: &MatterController, _args: &Map<String, Value>) -> Result<Value, CommandError> {
    let devices = c
        .stack
        .browse_commissionable(3000)
        .await
        .map_err(|e| stack_err(ServerErrorCode::SdkStackError, e))?;
    let out: Vec<Value> = devices
        .into_iter()
        .map(|d| {
            let (ip, port) = split_ip_port(&d.address);
            serde_json::to_value(CommissionableNodeData {
                instance_name: Some(d.instance_name),
                host_name: "000000000000".into(),
                port,
                long_discriminator: None,
                vendor_id: -1,
                product_id: -1,
                commissioning_mode: 1,
                device_type: None,
                device_name: None,
                pairing_instruction: None,
                pairing_hint: 0,
                mrp_retry_interval_idle: None,
                mrp_retry_interval_active: None,
                supports_tcp: false,
                addresses: vec![ip],
                rotating_id: None,
            })
            .unwrap()
        })
        .collect();
    Ok(Value::Array(out))
}

#[cfg(test)]
mod tests {
    use crate::real::test_rig::*;
    use crate::stack_api::{CommissionOutcome, WindowInfo};
    use serde_json::json;

    #[tokio::test]
    async fn commission_with_code_full_flow() {
        use crate::api::Controller;
        let r = rig();
        *r.stack.commission_response.lock().unwrap() =
            Some(Ok(CommissionOutcome { device_fabric_index: 3, address: "192.168.1.60:5540".into() }));
        *r.stack.interview_response.lock().unwrap() =
            Some(Ok([("0/40/2".to_string(), json!(65521))].into()));
        let mut events = r.ctrl.subscribe_events();
        let v = call(&r, "commission_with_code", json!({"code": "MT:TEST"})).await.unwrap();
        assert_eq!(v["node_id"], 1);
        assert_eq!(v["available"], false); // available flips when the supervisor connects
        assert_eq!(v["attributes"]["0/40/2"], 65521);
        let ev = events.recv().await.unwrap();
        assert_eq!(ev.event, "node_added");
        assert_eq!(ev.data["node_id"], 1);
        assert!(r.stack.calls().contains(&"start_supervisor 1".to_string()));
        // node id advanced + persisted
        let e = call(&r, "get_node", json!({"node_id": 1})).await;
        assert!(e.is_ok());
    }

    /// Task 19's live-device finding: rs-matter hands back an IPv6 peer as
    /// `"[fe80::1%14]:5540"`, and the brackets used to survive into the persisted
    /// record — so once `get_node_ip_addresses` cut the scope id off at `%`, a
    /// client got the unclosed literal `"[fe80::1"` and `ping_node` got something
    /// `ping6` refuses. Asserted end-to-end (commission -> record -> command)
    /// rather than only on the splitter, because the splitter's output is only
    /// wrong in the way that matters once a reader trims it.
    #[tokio::test]
    async fn a_commissioned_ipv6_peer_is_recorded_without_its_brackets() {
        let r = rig();
        *r.stack.commission_response.lock().unwrap() = Some(Ok(CommissionOutcome {
            device_fabric_index: 1,
            address: "[fe80::87f:8d29:2561:f7fb%14]:5540".into(),
        }));
        *r.stack.interview_response.lock().unwrap() = Some(Ok(Default::default()));
        call(&r, "commission_with_code", json!({"code": "MT:TEST"})).await.unwrap();

        // No live addresses: a restarted process only has the stored record, which
        // is exactly the case that exposed this.
        *r.stack.addresses_response.lock().unwrap() = Some(Ok(vec![]));
        let v = call(&r, "get_node_ip_addresses", json!({"node_id": 1})).await.unwrap();
        assert_eq!(v, json!(["fe80::87f:8d29:2561:f7fb"]));
        *r.stack.addresses_response.lock().unwrap() = Some(Ok(vec![]));
        let v = call(&r, "get_node_ip_addresses", json!({"node_id": 1, "scoped": true})).await.unwrap();
        assert_eq!(v, json!(["fe80::87f:8d29:2561:f7fb%14"]));
    }

    // The per-form unit test for the splitter moved with the function itself, to
    // `crate::addr`. The end-to-end case above is the one that belongs here.

    #[tokio::test]
    async fn commission_with_code_empty_code() {
        let r = rig();
        let e = call(&r, "commission_with_code", json!({"code": ""})).await.unwrap_err();
        assert_eq!(e.code.code(), 8);
        assert_eq!(e.details, "No pairing code provided");
    }

    #[tokio::test]
    async fn commission_failure_maps_to_code_1() {
        let r = rig();
        *r.stack.commission_response.lock().unwrap() = Some(Err(crate::stack_api::StackError::new(
            crate::stack_api::StackErrorKind::Busy,
            "device is busy (previous commissioning attempt may hold its failsafe for ~60s)")));
        let e = call(&r, "commission_with_code", json!({"code": "MT:TEST"})).await.unwrap_err();
        assert_eq!(e.code.code(), 1);
        assert!(e.details.starts_with("Commission failed: "));
        assert!(e.details.contains("busy"));
    }

    /// Important-3 regression: `ops::commission` classifies a mistyped pairing
    /// code and an unparseable `ip_addr` as `InvalidArguments`, and `do_commission`
    /// used to flatten every kind to 1 — two lines away from an *empty* code
    /// answering 8. The classification now survives, with 1 still the default.
    #[tokio::test]
    async fn commission_failure_keeps_the_stacks_error_classification() {
        use crate::stack_api::{StackError, StackErrorKind};
        for (kind, expected) in [
            (StackErrorKind::InvalidArguments, 8),
            (StackErrorKind::NodeUnreachable, 4),
            (StackErrorKind::Timeout, 1),
            (StackErrorKind::Sdk, 1),
            (StackErrorKind::Busy, 1),
        ] {
            let r = rig();
            *r.stack.commission_response.lock().unwrap() =
                Some(Err(StackError::new(kind, "nope")));
            let e = call(&r, "commission_with_code", json!({"code": "MT:BAD"})).await.unwrap_err();
            assert_eq!(e.code.code(), expected, "for {kind:?}");
            // The prefix HA surfaces is kept on every kind.
            assert_eq!(e.details, "Commission failed: nope", "for {kind:?}");
        }
    }

    #[tokio::test]
    async fn commission_interview_failure_removes_fabric_and_maps_to_code_1() {
        let r = rig();
        *r.stack.commission_response.lock().unwrap() =
            Some(Ok(CommissionOutcome { device_fabric_index: 7, address: "192.168.1.60:5540".into() }));
        *r.stack.interview_response.lock().unwrap() = Some(Err(crate::stack_api::StackError::new(
            crate::stack_api::StackErrorKind::Timeout, "interview timed out")));
        let e = call(&r, "commission_with_code", json!({"code": "MT:TEST"})).await.unwrap_err();
        assert_eq!(e.code.code(), 1);
        assert!(e.details.starts_with("Commission failed: "));
        assert!(e.details.contains("interview timed out"));
        assert!(r.stack.calls().iter().any(|c| c == "remove_device_fabric node=1 idx=7"));
        // best-effort cleanup only: the node must NOT have been registered
        let e2 = call(&r, "get_node", json!({"node_id": 1})).await.unwrap_err();
        assert_eq!(e2.code.code(), 5);
    }

    #[tokio::test]
    async fn commission_on_network_ip_addr_uses_address_target() {
        let r = rig();
        *r.stack.commission_response.lock().unwrap() =
            Some(Ok(CommissionOutcome { device_fabric_index: 1, address: "192.168.1.99:5540".into() }));
        call(&r, "commission_on_network",
            json!({"setup_pin_code": 20202021, "ip_addr": "192.168.1.99"})).await.unwrap();
        assert!(r.stack.calls().iter().any(|c| c.contains("address") && c.contains("addr=192.168.1.99:5540")));
    }

    #[tokio::test]
    async fn commission_on_network_link_local_ip_falls_back_to_on_network() {
        let r = rig();
        *r.stack.commission_response.lock().unwrap() =
            Some(Ok(CommissionOutcome { device_fabric_index: 1, address: "192.168.1.50:5540".into() }));
        call(&r, "commission_on_network",
            json!({"setup_pin_code": 20202021, "ip_addr": "fe80::1"})).await.unwrap();
        assert!(r.stack.calls().iter().any(|c| c.contains("onnetwork")));
    }

    #[tokio::test]
    async fn commission_on_network_long_discriminator_filter_reaches_stack() {
        let r = rig();
        *r.stack.commission_response.lock().unwrap() =
            Some(Ok(CommissionOutcome { device_fabric_index: 1, address: "192.168.1.50:5540".into() }));
        call(&r, "commission_on_network",
            json!({"setup_pin_code": 20202021, "filter_type": 2, "filter": 3840})).await.unwrap();
        assert!(r.stack.calls().iter().any(|c| c.contains("onnetwork") && c.contains("long=Some(3840)")));
    }

    #[tokio::test]
    async fn commission_on_network_filter_validation() {
        let r = rig();
        let e = call(&r, "commission_on_network", json!({"setup_pin_code": 20202021, "filter_type": 2})).await.unwrap_err();
        assert_eq!(e.details, "filter required for filter_type 2 (long discriminator)");
        let e = call(&r, "commission_on_network", json!({})).await.unwrap_err();
        assert_eq!(e.details, "No passcode provided");
    }

    #[tokio::test]
    async fn open_commissioning_window_shape() {
        let r = rig_with_nodes(vec![node_record(5)]);
        *r.stack.window_response.lock().unwrap() = Some(Ok(WindowInfo {
            setup_pin_code: 12345678, setup_manual_code: "36296231493".into(),
            setup_qr_code: "MT:ABC".into() }));
        let v = call(&r, "open_commissioning_window", json!({"node_id": 5})).await.unwrap();
        assert_eq!(v, json!({"setup_pin_code": 12345678, "setup_manual_code": "36296231493", "setup_qr_code": "MT:ABC"}));
        assert!(r.stack.calls().iter().any(|c| c == "ocw node=5 timeout=300"));
    }

    #[tokio::test]
    async fn discover_maps_defaults() {
        let r = rig();
        *r.stack.browse_response.lock().unwrap() = Some(Ok(vec![crate::stack_api::DiscoveredDevice {
            instance_name: "A5F15790B69D73D9".into(), address: "192.168.1.61:5540".into() }]));
        let v = call(&r, "discover_commissionable_nodes", json!({})).await.unwrap();
        assert_eq!(v[0]["host_name"], "000000000000");
        assert_eq!(v[0]["vendor_id"], -1);
        assert_eq!(v[0]["addresses"], json!(["192.168.1.61"]));
    }
}
