//! WiFi/Thread credential storage: set/remove named credentials and report
//! them back with secrets stripped. All mutations persist config and
//! broadcast a fresh `server_info_updated`.

use serde_json::{json, Map, Value};

use crate::api::CommandError;
use crate::commands::{invalid, opt_str, require_str};
use crate::real::MatterController;
use crate::storage::{validate_credential_id, validate_thread_dataset, WifiCredential};

pub async fn set_wifi(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let ssid = require_str(args, "ssid")?.to_string();
    let credentials = opt_str(args, "credentials").unwrap_or("").to_string();
    let id = opt_str(args, "id").unwrap_or("default").to_string();

    let mut cfg = c.config.lock().unwrap().clone();
    validate_credential_id(&id, cfg.wifi_credentials.keys().cloned()).map_err(invalid)?;

    let password = if credentials.is_empty() {
        match cfg.wifi_credentials.get(&id) {
            Some(existing) if existing.ssid == ssid => existing.password.clone(),
            _ => {
                return Err(invalid(
                    "WiFi password is required (omit it only to keep the existing password for an unchanged SSID)",
                ))
            }
        }
    } else {
        credentials
    };
    cfg.wifi_credentials.insert(id, WifiCredential { ssid, password });
    if let Err(e) = c.storage.save_config(&cfg) {
        tracing::error!("persist config: {e}");
    }
    *c.config.lock().unwrap() = cfg;
    c.broadcast_server_info_updated();
    Ok(json!({}))
}

pub async fn set_thread(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let dataset = require_str(args, "dataset")?.to_string();
    validate_thread_dataset(&dataset).map_err(invalid)?;
    let id = opt_str(args, "id").unwrap_or("default").to_string();

    let mut cfg = c.config.lock().unwrap().clone();
    validate_credential_id(&id, cfg.thread_datasets.keys().cloned()).map_err(invalid)?;
    cfg.thread_datasets.insert(id, dataset);
    if let Err(e) = c.storage.save_config(&cfg) {
        tracing::error!("persist config: {e}");
    }
    *c.config.lock().unwrap() = cfg;
    c.broadcast_server_info_updated();
    Ok(json!({}))
}

pub async fn remove_wifi(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let id = opt_str(args, "id").unwrap_or("default").to_string();
    let mut cfg = c.config.lock().unwrap().clone();
    cfg.wifi_credentials.remove(&id);
    if let Err(e) = c.storage.save_config(&cfg) {
        tracing::error!("persist config: {e}");
    }
    *c.config.lock().unwrap() = cfg;
    c.broadcast_server_info_updated();
    Ok(json!({}))
}

pub async fn remove_thread(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let id = opt_str(args, "id").unwrap_or("default").to_string();
    let mut cfg = c.config.lock().unwrap().clone();
    cfg.thread_datasets.remove(&id);
    if let Err(e) = c.storage.save_config(&cfg) {
        tracing::error!("persist config: {e}");
    }
    *c.config.lock().unwrap() = cfg;
    c.broadcast_server_info_updated();
    Ok(json!({}))
}

/// Tiny Thread-TLV walk: (type: u8, len: u8, value) triples. Only the two
/// tags Node surfaces are decoded; anything else (including parse failure)
/// is silently skipped.
fn thread_dataset_info(hex: &str) -> (Option<String>, Option<String>) {
    let Ok(bytes) = (0..hex.len()).step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
        .collect::<Result<Vec<u8>, _>>() else { return (None, None) };
    let (mut name, mut xpan) = (None, None);
    let mut i = 0usize;
    while i + 2 <= bytes.len() {
        let (t, l) = (bytes[i], bytes[i + 1] as usize);
        let Some(v) = bytes.get(i + 2..i + 2 + l) else { break };
        match t {
            0x03 => name = std::str::from_utf8(v).ok().map(String::from),
            0x02 => xpan = Some(v.iter().map(|b| format!("{b:02X}")).collect()),
            _ => {}
        }
        i += 2 + l;
    }
    (name, xpan)
}

pub async fn get_all(c: &MatterController, _args: &Map<String, Value>) -> Result<Value, CommandError> {
    let cfg = c.config_snapshot();

    let mut wifi: Vec<Value> = cfg.wifi_credentials.iter()
        .map(|(id, cred)| json!({"id": id, "ssid": cred.ssid}))
        .collect();
    if !cfg.wifi_credentials.contains_key("default") {
        wifi.insert(0, json!({"id": "default", "ssid": ""}));
    }

    let mut thread: Vec<Value> = cfg.thread_datasets.iter()
        .map(|(id, dataset)| {
            let (name, xpan) = thread_dataset_info(dataset);
            let mut obj = Map::new();
            obj.insert("id".into(), json!(id));
            if let Some(n) = name { obj.insert("networkName".into(), json!(n)); }
            if let Some(x) = xpan { obj.insert("extPanId".into(), json!(x)); }
            Value::Object(obj)
        })
        .collect();
    if !cfg.thread_datasets.contains_key("default") {
        thread.insert(0, json!({"id": "default"}));
    }

    Ok(json!({"wifi": wifi, "thread": thread}))
}

#[cfg(test)]
mod tests {
    use crate::real::test_rig::*;
    use serde_json::json;

    #[tokio::test]
    async fn wifi_credentials_set_get_remove_and_server_info() {
        use crate::api::Controller;
        let r = rig();
        let mut events = r.ctrl.subscribe_events();
        let v = call(&r, "set_wifi_credentials", json!({"ssid": "iot", "credentials": "hunter2"})).await.unwrap();
        assert_eq!(v, json!({}));
        assert_eq!(events.recv().await.unwrap().event, "server_info_updated");
        let si = call(&r, "server_info", json!({})).await.unwrap();
        assert_eq!(si["wifi_credentials_set"], true);
        assert_eq!(si["wifi_ssid"], "iot");
        let v = call(&r, "get_all_credentials", json!({})).await.unwrap();
        assert_eq!(v["wifi"], json!([{"id": "default", "ssid": "iot"}]));
        // secrets are write-only: password never appears
        assert!(!v.to_string().contains("hunter2"));
        call(&r, "remove_wifi_credentials", json!({})).await.unwrap();
        let si = call(&r, "server_info", json!({})).await.unwrap();
        assert_eq!(si["wifi_credentials_set"], false);
    }

    #[tokio::test]
    async fn wifi_password_required_unless_same_ssid() {
        let r = rig();
        let e = call(&r, "set_wifi_credentials", json!({"ssid": "iot", "credentials": ""})).await.unwrap_err();
        assert_eq!(e.details, "WiFi password is required (omit it only to keep the existing password for an unchanged SSID)");
        call(&r, "set_wifi_credentials", json!({"ssid": "iot", "credentials": "pw"})).await.unwrap();
        // same ssid, empty password -> keeps old
        call(&r, "set_wifi_credentials", json!({"ssid": "iot", "credentials": ""})).await.unwrap();
        let si = call(&r, "server_info", json!({})).await.unwrap();
        assert_eq!(si["wifi_credentials_set"], true);
    }

    #[tokio::test]
    async fn thread_dataset_validation_and_decode() {
        let r = rig();
        let e = call(&r, "set_thread_dataset", json!({"dataset": "xyz"})).await.unwrap_err();
        assert_eq!(e.code.code(), 8);
        // TLVs: 0x02 (ExtPanId) len 8; 0x03 (NetworkName) len 4 "test"
        let ds = "0208deadbeefcafe0001030474657374";
        call(&r, "set_thread_dataset", json!({"dataset": ds})).await.unwrap();
        let v = call(&r, "get_all_credentials", json!({})).await.unwrap();
        assert_eq!(v["thread"][0]["id"], "default");
        assert_eq!(v["thread"][0]["networkName"], "test");
        assert_eq!(v["thread"][0]["extPanId"], "DEADBEEFCAFE0001");
    }

    #[tokio::test]
    async fn named_credentials_and_reserved_ids() {
        let r = rig();
        call(&r, "set_wifi_credentials", json!({"ssid": "a", "credentials": "b", "id": "garage"})).await.unwrap();
        let e = call(&r, "set_wifi_credentials", json!({"ssid": "a", "credentials": "b", "id": "GARAGE"})).await.unwrap_err();
        assert_eq!(e.details, "invalid-credential-id: 'GARAGE' duplicates existing 'garage'");
        let e = call(&r, "set_wifi_credentials", json!({"ssid": "a", "credentials": "b", "id": "delete"})).await.unwrap_err();
        assert_eq!(e.details, "invalid-credential-id: 'delete' is reserved");
        let v = call(&r, "get_all_credentials", json!({})).await.unwrap();
        // default force-prepended even though only "garage" exists
        assert_eq!(v["wifi"][0]["id"], "default");
        assert_eq!(v["wifi"][1]["id"], "garage");
    }
}
