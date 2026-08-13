use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: u8 = 13;
pub const MIN_SUPPORTED_SCHEMA_VERSION: u8 = 11;

/// Pushed bare (unenveloped) on WS connect; also the result of `server_info`.
/// Field set mirrors matterjs-server's ServerInfoMessage (ws-client model.ts).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerInfoMessage {
    pub fabric_id: u64,
    pub compressed_fabric_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fabric_index: Option<u8>,
    pub schema_version: u8,
    pub min_supported_schema_version: u8,
    pub sdk_version: String,
    pub wifi_credentials_set: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wifi_ssid: Option<String>,
    pub thread_credentials_set: bool,
    pub bluetooth_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ble_proxy_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub controller_node_id: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_like_node_server_and_skips_absent_optionals() {
        let info = ServerInfoMessage {
            fabric_id: 1,
            compressed_fabric_id: 9876543210,
            fabric_index: None,
            schema_version: SCHEMA_VERSION,
            min_supported_schema_version: MIN_SUPPORTED_SCHEMA_VERSION,
            sdk_version: "matter-rs-server/0.1.0 (rs-matter/03bc8f2)".into(),
            wifi_credentials_set: false,
            wifi_ssid: None,
            thread_credentials_set: false,
            bluetooth_enabled: false,
            ble_proxy_enabled: None,
            controller_node_id: None,
        };
        let v: serde_json::Value = serde_json::to_value(&info).unwrap();
        assert_eq!(v["schema_version"], 13);
        assert_eq!(v["min_supported_schema_version"], 11);
        assert_eq!(v["compressed_fabric_id"], serde_json::json!(9876543210u64));
        assert!(v.get("fabric_index").is_none());
        assert!(v.get("wifi_ssid").is_none());
    }
}
