use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The python-matter-server MatterNodeData shape (schema 13).
/// `attributes` keys are decimal "endpoint/cluster/attribute" paths; values
/// are tag-based JSON. `interview_version` is a compat constant (always 6).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatterNodeData {
    pub node_id: u64,
    pub date_commissioned: String,
    pub last_interview: String,
    pub interview_version: u8,
    pub available: bool,
    pub is_bridge: bool,
    pub attributes: serde_json::Map<String, Value>,
    pub attribute_subscriptions: Vec<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matter_version: Option<String>,
}

/// node_event payload. `data` is name-based (camelCase) or Null.
/// timestamp_type: 1 = epoch, 0 = system, 2 = POSIX fallback (Node behavior).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatterNodeEvent {
    pub node_id: u64,
    pub endpoint_id: u16,
    pub cluster_id: u32,
    pub event_id: u32,
    pub event_number: u64,
    pub priority: u8,
    pub timestamp: i64,
    pub timestamp_type: u8,
    pub data: Value,
}

/// discover / discover_commissionable_nodes entry. Field defaults mirror the
/// Node server (host_name hardcoded, product_id -1 when unknown).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommissionableNodeData {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_name: Option<String>,
    pub host_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub long_discriminator: Option<u16>,
    pub vendor_id: i32,
    pub product_id: i32,
    pub commissioning_mode: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_type: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pairing_instruction: Option<String>,
    pub pairing_hint: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mrp_retry_interval_idle: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mrp_retry_interval_active: Option<u32>,
    pub supports_tcp: bool,
    pub addresses: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rotating_id: Option<String>,
}

/// get_matter_fabrics entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatterFabricData {
    pub fabric_id: u64,
    pub vendor_id: u16,
    pub fabric_index: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fabric_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vendor_name: Option<String>,
}

/// get_icd_state / register_icd / unregister_icd result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcdState {
    pub supported: bool,
    pub lit_supported: bool,
    pub registered: bool,
    pub operating_mode: Option<String>,
    pub awake: Option<bool>,
    pub available: Option<bool>,
    pub next_expected_checkin: Option<i64>,
}

impl IcdState {
    /// The honest-stub "not registered / not supported" shape.
    pub fn not_registered() -> Self {
        Self { supported: false, lit_supported: false, registered: false,
               operating_mode: None, awake: None, available: None, next_expected_checkin: None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn matter_node_data_shape() {
        let mut attributes = serde_json::Map::new();
        attributes.insert("0/40/2".into(), json!(65521));
        attributes.insert("1/6/0".into(), json!(true));
        let n = MatterNodeData {
            node_id: 4,
            date_commissioned: "2026-08-13T10:15:42.123000".into(),
            last_interview: "2026-08-13T10:15:42.123000".into(),
            interview_version: 6,
            available: true,
            is_bridge: false,
            attributes,
            attribute_subscriptions: vec![],
            matter_version: None,
        };
        let v = serde_json::to_value(&n).unwrap();
        assert_eq!(v["node_id"], 4);
        assert_eq!(v["interview_version"], 6);
        assert_eq!(v["attributes"]["1/6/0"], true);
        assert_eq!(v["attribute_subscriptions"], json!([]));
        assert!(v.get("matter_version").is_none());
    }

    #[test]
    fn node_event_shape() {
        let e = MatterNodeEvent {
            node_id: 1, endpoint_id: 1, cluster_id: 59, event_id: 1,
            event_number: 12345, priority: 1, timestamp: 1704067200000,
            timestamp_type: 1, data: json!({"newPosition": 1}),
        };
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["event_number"], 12345);
        assert_eq!(v["data"]["newPosition"], 1);
    }

    #[test]
    fn icd_state_not_registered_default() {
        let v = serde_json::to_value(IcdState::not_registered()).unwrap();
        assert_eq!(v["supported"], false);
        assert_eq!(v["registered"], false);
        assert_eq!(v["operating_mode"], serde_json::Value::Null);
        assert_eq!(v["next_expected_checkin"], serde_json::Value::Null);
    }

    #[test]
    fn fabric_data_skips_absent_vendor_name() {
        let f = MatterFabricData { fabric_id: 1, vendor_id: 0xFFF1, fabric_index: 1,
                                   fabric_label: Some("HomeAssistant".into()), vendor_name: None };
        let v = serde_json::to_value(&f).unwrap();
        assert!(v.get("vendor_name").is_none());
        assert_eq!(v["fabric_label"], "HomeAssistant");
    }
}
