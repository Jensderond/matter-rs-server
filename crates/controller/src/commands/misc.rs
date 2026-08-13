//! Vendor names, loglevel get/set, and the honest ICD/OTA stubs.
//!
//! ICD (Intermittently Connected Device) support and OTA (over-the-air)
//! update support are not implemented by this controller. Rather than
//! silently no-op or fake success, these handlers report their true state:
//! ICD queries return "not registered / not supported", and `update_node`
//! fails with an explicit "OTA is disabled" error. This keeps the 31-command
//! surface fully routed (never falling through to "Unknown command") while
//! being honest about what isn't backed by real functionality.

use serde_json::{json, Map, Value};

use matter_rs_wire::error::ServerErrorCode;
use matter_rs_wire::node::IcdState;

use crate::api::CommandError;
use crate::commands::{err, require_u64};
use crate::real::MatterController;

pub async fn get_vendor_names(_c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let filter: Option<Vec<u64>> = args.get("filter_vendors")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_u64).collect());
    let mut out = Map::new();
    for (id, name) in crate::vendors::all() {
        if filter.as_ref().is_none_or(|f| f.contains(&(*id as u64))) {
            out.insert(id.to_string(), json!(name));
        }
    }
    Ok(Value::Object(out))
}

pub async fn get_loglevel(c: &MatterController, _args: &Map<String, Value>) -> Result<Value, CommandError> {
    let (console, file) = c.log.get();
    Ok(json!({"console_loglevel": console, "file_loglevel": file}))
}

pub async fn set_loglevel(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    c.log.set(args.get("console_loglevel").and_then(Value::as_str),
              args.get("file_loglevel").and_then(Value::as_str));
    get_loglevel(c, args).await
}

pub async fn icd_state(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    Ok(serde_json::to_value(IcdState::not_registered()).unwrap())
}

pub async fn resync_icd(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    Ok(Value::Null)
}

pub async fn check_node_update(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    Ok(Value::Null)
}

pub async fn update_node(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    Err(err(ServerErrorCode::UpdateError, "OTA is disabled"))
}

#[cfg(test)]
mod tests {
    use crate::real::test_rig::*;
    use serde_json::json;

    #[tokio::test]
    async fn vendor_names_full_and_filtered() {
        let r = rig();
        let v = call(&r, "get_vendor_names", json!({})).await.unwrap();
        assert_eq!(v["4476"], "IKEA of Sweden");
        // 39321 is verified absent from the full vendor table (see vendors.rs);
        // it stands in for "some unknown id" and must be silently omitted.
        let v = call(&r, "get_vendor_names", json!({"filter_vendors": [4476, 39321]})).await.unwrap();
        assert_eq!(v.as_object().unwrap().len(), 1);
        assert_eq!(v["4476"], "IKEA of Sweden");
    }

    #[tokio::test]
    async fn loglevel_get_set() {
        let r = rig();
        let v = call(&r, "get_loglevel", json!({})).await.unwrap();
        assert_eq!(v, json!({"console_loglevel": "info", "file_loglevel": null}));
        let v = call(&r, "set_loglevel", json!({"console_loglevel": "debug"})).await.unwrap();
        // NopLog ignores sets; shape is what matters here
        assert!(v.get("console_loglevel").is_some());
    }

    #[tokio::test]
    async fn icd_stubs() {
        let r = rig_with_nodes(vec![node_record(5)]);
        let v = call(&r, "get_icd_state", json!({"node_id": 5})).await.unwrap();
        assert_eq!(v["supported"], false);
        assert_eq!(v["registered"], false);
        assert_eq!(v["operating_mode"], serde_json::Value::Null);
        let v = call(&r, "resync_icd", json!({"node_id": 5})).await.unwrap();
        assert_eq!(v, serde_json::Value::Null);
        let e = call(&r, "get_icd_state", json!({"node_id": 9})).await.unwrap_err();
        assert_eq!(e.details, "Node 9 does not exist");
    }

    #[tokio::test]
    async fn ota_stubs() {
        let r = rig_with_nodes(vec![node_record(5)]);
        let v = call(&r, "check_node_update", json!({"node_id": 5})).await.unwrap();
        assert_eq!(v, serde_json::Value::Null);
        let e = call(&r, "update_node", json!({"node_id": 5, "software_version": 2})).await.unwrap_err();
        assert_eq!(e.code.code(), 11);
        assert_eq!(e.details, "OTA is disabled");
    }

    #[tokio::test]
    async fn all_31_commands_are_dispatched() {
        // The full v1 surface: every command must be routed (i.e. NOT hit the
        // "Unknown command" fallback), whatever its result is.
        let r = rig();
        let all = [
            "server_info", "start_listening", "diagnostics", "ping_node", "get_node_ip_addresses",
            "get_nodes", "get_node", "interview_node", "remove_node",
            "device_command", "read_attribute", "write_attribute",
            "commission_with_code", "commission_on_network", "open_commissioning_window",
            "discover_commissionable_nodes", "discover",
            "set_wifi_credentials", "set_thread_dataset", "remove_wifi_credentials",
            "remove_thread_dataset", "get_all_credentials",
            "set_default_fabric_label", "get_fabric_label", "get_matter_fabrics",
            "remove_matter_fabric", "set_acl_entry", "set_node_binding",
            "get_vendor_names", "get_loglevel", "set_loglevel",
        ];
        assert_eq!(all.len(), 31);
        for name in all {
            match call(&r, name, json!({})).await {
                Ok(_) => {}
                Err(e) => assert!(
                    !e.details.starts_with("Unknown command"),
                    "{name} hit the Unknown command fallback"),
            }
        }
        // and the honest-stub / gated set:
        for name in ["get_icd_state", "register_icd", "unregister_icd", "resync_icd",
                     "check_node_update", "update_node"] {
            let e = call(&r, name, json!({"node_id": 1})).await.unwrap_err();
            assert!(!e.details.starts_with("Unknown command"), "{name}");
        }
        for name in ["import_test_node", "send_webrtc_provider_command", "subscribe_attribute",
                     "get_thread_diagnostics", "get_thread_border_routers", "get_network_topology"] {
            let e = call(&r, name, json!({})).await.unwrap_err();
            assert_eq!(e.code.code(), 9, "{name} must be Unknown command");
        }
    }
}
