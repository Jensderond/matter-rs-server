//! Commissioning and discovery command family: PASE commissioning over a
//! pairing code or on-network filter, opening a commissioning window, and
//! mDNS discovery of commissionable devices.

use serde_json::{json, Map, Value};

use matter_rs_wire::error::ServerErrorCode;
use matter_rs_wire::node::CommissionableNodeData;

use crate::api::CommandError;
use crate::commands::{err, invalid, opt_str, opt_u64, require_u64, stack_err};
use crate::real::MatterController;
use crate::stack_api::{CommissionRequest, PaseTarget};
use crate::storage::{allocate_node_id, format_node_date, NodeRecord};

/// Splits `"ip:port"` into `(ip, Some(port))`, unwrapping the brackets an IPv6
/// socket address carries; passes through unchanged (`None` port) when there is
/// no port to strip.
///
/// The bracket branch has to come first. rs-matter renders an IPv6 peer as
/// `"[fe80::1%14]:5540"`, and a bare `rsplit_once(':')` on that leaves the
/// brackets on the host — which is what used to get persisted into
/// `NodeRecord::addresses`, so `get_node_ip_addresses` (which cuts the scope id
/// off at `%`) answered a client with the unclosed literal `"[fe80::1"` and
/// `ping_node` handed `ping6` an address it cannot parse.
///
/// `stack::ops::ip_of` does the same job for the live-address path and the
/// duplication is deliberate: `controller` cannot depend on `stack`, the
/// dependency runs the other way.
fn split_ip_port(address: &str) -> (String, Option<u16>) {
    if let Some(rest) = address.strip_prefix('[') {
        return match rest.split_once(']') {
            // The host is exactly what the brackets held, whether or not a
            // `:port` follows.
            Some((host, tail)) => {
                (host.to_string(), tail.strip_prefix(':').and_then(|p| p.parse().ok()))
            }
            // Unterminated bracket: not something rs-matter produces, but
            // returning the remainder beats returning the `[`.
            None => (rest.to_string(), None),
        };
    }
    // Unbracketed. More than one colon means an IPv6 literal written without
    // brackets, which therefore cannot carry a port — take it whole.
    if address.matches(':').count() > 1 {
        return (address.to_string(), None);
    }
    match address.rsplit_once(':') {
        Some((ip, port)) => (ip.to_string(), port.parse().ok()),
        None => (address.to_string(), None),
    }
}

/// Shared commission -> interview -> persist -> supervise flow, used by both
/// `commission_with_code` and `commission_on_network`. Holds `alloc_lock` for
/// the whole flow: it serializes node-id allocation the same way Node's
/// mutex does, and PASE commissioning serializes upstream anyway.
async fn do_commission(c: &MatterController, target: PaseTarget) -> Result<Value, CommandError> {
    let _guard = c.alloc_lock.lock().await;

    // Allocate + persist the node id BEFORE commissioning (never hold the
    // std config Mutex across an await: update_config clones, mutates,
    // saves, and writes back synchronously, all before the await below).
    let node_id = c.update_config(|cfg| {
        allocate_node_id(cfg, |id| c.registry.contains(id) || id == c.identity.controller_node_id)
    });
    let fabric_label = c.config_snapshot().fabric_label;

    let req = CommissionRequest { node_id, target, fabric_label };
    let outcome = c.stack.commission(req).await.map_err(|e| {
        err(ServerErrorCode::NodeCommissionFailed, format!("Commission failed: {}", e.message))
    })?;

    let attributes = match c.stack.interview(node_id).await {
        Ok(a) => a,
        Err(e) => {
            let _ = c.stack.remove_device_fabric(node_id, outcome.device_fabric_index).await;
            return Err(err(
                ServerErrorCode::NodeCommissionFailed,
                format!("Commission failed: {}", e.message),
            ));
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
    let passcode = opt_u64(args, "setup_pin_code")
        .map(|v| v as u32)
        .ok_or_else(|| invalid("No passcode provided"))?;
    let filter_type = opt_u64(args, "filter_type");
    let filter = opt_u64(args, "filter");

    let (mut short_discriminator, mut long_discriminator, mut vendor_id) = (None, None, None);
    match filter_type {
        Some(1) => {
            short_discriminator = Some(
                filter.ok_or_else(|| invalid("filter required for filter_type 1 (short discriminator)"))? as u8,
            );
        }
        Some(2) => {
            long_discriminator = Some(
                filter.ok_or_else(|| invalid("filter required for filter_type 2 (long discriminator)"))? as u16,
            );
        }
        Some(3) => {
            vendor_id =
                Some(filter.ok_or_else(|| invalid("filter required for filter_type 3 (vendor ID)"))? as u16);
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
    let timeout = opt_u64(args, "timeout").unwrap_or(300) as u16;
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

    #[test]
    fn split_ip_port_handles_every_address_form_rs_matter_produces() {
        use super::split_ip_port;
        let cases: &[(&str, (&str, Option<u16>))] = &[
            ("192.168.1.60:5540", ("192.168.1.60", Some(5540))),
            ("192.168.1.60", ("192.168.1.60", None)),
            ("[fe80::1%14]:5540", ("fe80::1%14", Some(5540))),
            ("[fd12::5]:5540", ("fd12::5", Some(5540))),
            ("[fd12::5]", ("fd12::5", None)),
            // Not shapes rs-matter emits, but they must not produce a `[` either.
            ("[fd12::5", ("fd12::5", None)),
            ("fe80::1", ("fe80::1", None)),
            ("192.168.1.60:notaport", ("192.168.1.60", None)),
        ];
        for (input, (ip, port)) in cases {
            let got = split_ip_port(input);
            assert_eq!((got.0.as_str(), got.1), (*ip, *port), "for {input:?}");
            assert!(!got.0.contains('['), "for {input:?}");
        }
    }

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
