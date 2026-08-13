//! Fabric-label ownership, device-side fabric listing/removal, and the two
//! Node quirks kept for compatibility: tag-based ACL entries and node
//! bindings written through `write_attribute`.

use serde_json::{json, Map, Value};

use matter_rs_wire::error::ServerErrorCode;
use matter_rs_wire::node::MatterFabricData;

use crate::api::{CommandError, ConnId};
use crate::commands::{err, invalid, require_u64, stack_err};
use crate::real::MatterController;
use crate::storage::normalize_fabric_label;

pub async fn set_default_fabric_label(
    c: &MatterController,
    conn: ConnId,
    args: &Map<String, Value>,
) -> Result<Value, CommandError> {
    if c.label_locked {
        tracing::info!(
            "Ignoring set_default_fabric_label (pinned via --default-fabric-label)"
        );
        return Ok(Value::Null);
    }

    let mut claimed_fresh = false;
    {
        let mut owner = c.label_owner.lock().unwrap();
        match *owner {
            None => {
                *owner = Some(conn);
                claimed_fresh = true;
            }
            Some(other) if other != conn => {
                tracing::info!("Ignoring set_default_fabric_label (owned by another connection)");
                return Ok(Value::Null);
            }
            _ => {}
        }
    }

    let label_arg = args.get("label").and_then(Value::as_str);
    let label = normalize_fabric_label(label_arg);
    match c.stack.update_fabric_label(label.clone()).await {
        Ok(()) => {
            let mut cfg = c.config.lock().unwrap().clone();
            cfg.fabric_label = label;
            if let Err(e) = c.storage.save_config(&cfg) {
                tracing::error!("persist config: {e}");
            }
            *c.config.lock().unwrap() = cfg;
            Ok(Value::Null)
        }
        Err(e) => {
            if claimed_fresh {
                let mut owner = c.label_owner.lock().unwrap();
                if *owner == Some(conn) {
                    *owner = None;
                }
            }
            Err(err(ServerErrorCode::SdkStackError, e.message))
        }
    }
}

pub async fn get_fabric_label(c: &MatterController, _args: &Map<String, Value>) -> Result<Value, CommandError> {
    Ok(json!({"fabric_label": c.config_snapshot().fabric_label}))
}

pub async fn get_matter_fabrics(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    let fabrics = c.stack.device_fabrics(node_id).await.map_err(|_| {
        err(ServerErrorCode::SdkStackError, "No or invalid response received while querying fabrics")
    })?;
    let out: Vec<Value> = fabrics
        .into_iter()
        .map(|f| {
            serde_json::to_value(MatterFabricData {
                fabric_id: f.fabric_id,
                vendor_id: f.vendor_id,
                fabric_index: f.fabric_index,
                fabric_label: Some(f.fabric_label),
                vendor_name: crate::vendors::name(f.vendor_id),
            })
            .unwrap()
        })
        .collect();
    Ok(Value::Array(out))
}

pub async fn remove_matter_fabric(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    let fabric_index = require_u64(args, "fabric_index")? as u8;
    c.stack
        .remove_device_fabric(node_id, fabric_index)
        .await
        .map_err(|e| stack_err(ServerErrorCode::SdkStackError, e))?;
    Ok(json!({}))
}

/// AccessControlEntryStruct context tags: privilege=1, authMode=2,
/// subjects=3, targets=4; target struct: cluster=0, endpoint=1, deviceType=2.
/// Node quirk kept: subjects equal to the target node's own id are dropped;
/// an entry whose subjects list becomes empty as a result is dropped
/// entirely (null-subject entries are untouched). Still reports success.
fn map_acl_entry(entry: &Value, node_id: u64) -> Option<Value> {
    let obj = entry.as_object()?;
    let mut out = Map::new();
    if let Some(v) = obj.get("privilege") { out.insert("1".into(), v.clone()); }
    if let Some(v) = obj.get("auth_mode") { out.insert("2".into(), v.clone()); }

    match obj.get("subjects") {
        None | Some(Value::Null) => { out.insert("3".into(), Value::Null); }
        Some(Value::Array(subjects)) => {
            let filtered: Vec<Value> = subjects.iter()
                .filter(|s| s.as_u64() != Some(node_id))
                .cloned()
                .collect();
            if filtered.is_empty() { return None; }
            out.insert("3".into(), Value::Array(filtered));
        }
        Some(other) => { out.insert("3".into(), other.clone()); }
    }

    match obj.get("targets") {
        None | Some(Value::Null) => { out.insert("4".into(), Value::Null); }
        Some(Value::Array(targets)) => {
            let mapped: Vec<Value> = targets.iter().map(map_acl_target).collect();
            out.insert("4".into(), Value::Array(mapped));
        }
        Some(other) => { out.insert("4".into(), other.clone()); }
    }

    Some(Value::Object(out))
}

fn map_acl_target(target: &Value) -> Value {
    let obj = target.as_object();
    let get = |key: &str| obj.and_then(|o| o.get(key)).cloned().unwrap_or(Value::Null);
    let mut out = Map::new();
    out.insert("0".into(), get("cluster"));
    out.insert("1".into(), get("endpoint"));
    out.insert("2".into(), get("device_type"));
    Value::Object(out)
}

pub async fn set_acl_entry(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    let entries = args.get("entry").and_then(Value::as_array)
        .ok_or_else(|| invalid("missing or invalid required argument: entry"))?;

    let mapped: Vec<Value> = entries.iter().filter_map(|e| map_acl_entry(e, node_id)).collect();

    let status = c.stack.write_attribute(node_id, 0, 31, 0, Value::Array(mapped)).await
        .map_err(|e| stack_err(ServerErrorCode::SdkStackError, e))?;
    Ok(json!([{"path": {"endpoint_id": 0, "cluster_id": 31, "attribute_id": 0}, "status": status}]))
}

/// TargetStruct tags: node=1, group=2, endpoint=3, cluster=4. Nulls are
/// omitted (unlike the ACL mapping above, which preserves them).
fn map_binding(binding: &Value) -> Value {
    let obj = binding.as_object();
    let mut out = Map::new();
    let mut put = |tag: &str, key: &str| {
        if let Some(v) = obj.and_then(|o| o.get(key)) {
            if !v.is_null() { out.insert(tag.into(), v.clone()); }
        }
    };
    put("1", "node");
    put("2", "group");
    put("3", "endpoint");
    put("4", "cluster");
    Value::Object(out)
}

pub async fn set_node_binding(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    let endpoint = require_u64(args, "endpoint")? as u16;
    let bindings = args.get("bindings").and_then(Value::as_array)
        .ok_or_else(|| invalid("missing or invalid required argument: bindings"))?;

    let mapped: Vec<Value> = bindings.iter().map(map_binding).collect();

    let status = c.stack.write_attribute(node_id, endpoint, 30, 0, Value::Array(mapped)).await
        .map_err(|e| stack_err(ServerErrorCode::SdkStackError, e))?;
    Ok(json!([{"path": {"endpoint_id": endpoint, "cluster_id": 30, "attribute_id": 0}, "status": status}]))
}

#[cfg(test)]
mod tests {
    use crate::api::{ConnId, Controller};
    use crate::real::test_rig::*;
    use serde_json::json;

    #[tokio::test]
    async fn fabric_label_ownership_per_connection() {
        let r = rig();
        // conn 1 claims
        let v = r.ctrl.handle_command(ConnId(1), &cmd("set_default_fabric_label", json!({"label": "Casa"}))).await.unwrap();
        assert_eq!(v, serde_json::Value::Null);
        assert!(r.stack.calls().contains(&"update_fabric_label Casa".to_string()));
        // conn 2 is ignored but still succeeds
        r.ctrl.handle_command(ConnId(2), &cmd("set_default_fabric_label", json!({"label": "Nope"}))).await.unwrap();
        let v = call(&r, "get_fabric_label", json!({})).await.unwrap();
        assert_eq!(v, json!({"fabric_label": "Casa"}));
        // conn 1 closing releases ownership; conn 2 can now set
        r.ctrl.connection_closed(ConnId(1));
        r.ctrl.handle_command(ConnId(2), &cmd("set_default_fabric_label", json!({"label": "Second"}))).await.unwrap();
        let v = call(&r, "get_fabric_label", json!({})).await.unwrap();
        assert_eq!(v, json!({"fabric_label": "Second"}));
    }

    #[tokio::test]
    async fn empty_label_resets_to_homeassistant() {
        let r = rig();
        call(&r, "set_default_fabric_label", json!({"label": ""})).await.unwrap();
        let v = call(&r, "get_fabric_label", json!({})).await.unwrap();
        assert_eq!(v, json!({"fabric_label": "HomeAssistant"}));
    }

    #[tokio::test]
    async fn get_matter_fabrics_maps_device_list() {
        let r = rig_with_nodes(vec![node_record(5)]);
        *r.stack.fabrics_response.lock().unwrap() = Some(Ok(vec![crate::stack_api::DeviceFabric {
            fabric_id: 1, vendor_id: 0xFFF1, fabric_index: 3, fabric_label: "HomeAssistant".into() }]));
        let v = call(&r, "get_matter_fabrics", json!({"node_id": 5})).await.unwrap();
        assert_eq!(v[0]["fabric_index"], 3);
        assert_eq!(v[0]["fabric_label"], "HomeAssistant");
        let v = call(&r, "remove_matter_fabric", json!({"node_id": 5, "fabric_index": 3})).await.unwrap();
        assert_eq!(v, json!({}));
    }

    #[tokio::test]
    async fn set_acl_entry_strips_self_subjects_and_writes_tag_based() {
        let r = rig_with_nodes(vec![node_record(5)]);
        let v = call(&r, "set_acl_entry", json!({"node_id": 5, "entry": [
            {"privilege": 5, "auth_mode": 2, "subjects": [112233, 5], "targets": null},
            {"privilege": 3, "auth_mode": 2, "subjects": [5], "targets": null}
        ]})).await.unwrap();
        // second entry lost its only subject (self) and was dropped; still success
        assert_eq!(v, json!([{"path": {"endpoint_id": 0, "cluster_id": 31, "attribute_id": 0}, "status": 0}]));
        assert!(r.stack.calls().iter().any(|c| c == "write node=5 0/31/0"));
    }

    #[tokio::test]
    async fn set_node_binding_writes_binding_cluster() {
        let r = rig_with_nodes(vec![node_record(5)]);
        let v = call(&r, "set_node_binding", json!({"node_id": 5, "endpoint": 1,
            "bindings": [{"node": 2, "group": null, "endpoint": 1, "cluster": 6}]})).await.unwrap();
        assert_eq!(v, json!([{"path": {"endpoint_id": 1, "cluster_id": 30, "attribute_id": 0}, "status": 0}]));
        assert!(r.stack.calls().iter().any(|c| c == "write node=5 1/30/0"));
    }
}
