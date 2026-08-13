//! Plain-JSON storage under --storage-path. All writes atomic (tmp + rename,
//! same directory). server.json / config.json carry key material -> 0600.
//! nodes/<id>.json holds the served attribute cache verbatim.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

mod b64 {
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(v))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        base64::engine::general_purpose::STANDARD.decode(s).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerIdentity {
    pub fabric_id: u64,
    pub vendor_id: u16,
    pub controller_node_id: u64,
    pub compressed_fabric_id: u64,
    #[serde(with = "b64")] pub ca_private_key: Vec<u8>,
    #[serde(with = "b64")] pub rcac_tlv: Vec<u8>,
    #[serde(with = "b64")] pub controller_private_key: Vec<u8>,
    #[serde(with = "b64")] pub controller_noc_tlv: Vec<u8>,
    #[serde(with = "b64")] pub ipk: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WifiCredential { pub ssid: String, pub password: String }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ConfigData {
    pub fabric_label: String,
    pub next_node_id: u64,
    pub wifi_credentials: BTreeMap<String, WifiCredential>,
    pub thread_datasets: BTreeMap<String, String>,
}

impl Default for ConfigData {
    fn default() -> Self {
        Self { fabric_label: "HomeAssistant".into(), next_node_id: 1,
               wifi_credentials: BTreeMap::new(), thread_datasets: BTreeMap::new() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRecord {
    pub node_id: u64,
    pub date_commissioned: String,
    pub last_interview: String,
    pub device_fabric_index: u8,
    #[serde(default)]
    pub addresses: Vec<String>,
    #[serde(default)]
    pub attributes: serde_json::Map<String, Value>,
}

pub struct Storage { root: PathBuf }

impl Storage {
    pub fn open(root: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(root.join("nodes"))?;
        std::fs::create_dir_all(root.join("sessions"))?;
        Ok(Self { root: root.to_path_buf() })
    }

    pub fn root(&self) -> &Path { &self.root }

    /// Three-state on purpose: `Ok(None)` ONLY when server.json does not exist.
    /// An unreadable or unparseable file is an `Err`, never `None` — a caller
    /// that read "no identity yet" out of a corrupt file would mint a new
    /// fabric and rename over key material that is still recoverable by hand,
    /// orphaning every commissioned node.
    pub fn load_identity(&self) -> io::Result<Option<ServerIdentity>> {
        read_json_strict(&self.root.join("server.json"))
    }
    /// First write of the identity. Refuses to clobber an existing file, so a
    /// logic slip upstream stays recoverable instead of destroying the CA key.
    pub fn create_identity(&self, id: &ServerIdentity) -> io::Result<()> {
        let path = self.root.join("server.json");
        if path.try_exists()? {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("refusing to overwrite existing {path:?} with a freshly generated identity"),
            ));
        }
        write_json_atomic(&path, id, true)
    }
    /// Rewrite an identity that is already on disk (corrections to derived
    /// fields). Use `create_identity` for the first write.
    pub fn save_identity(&self, id: &ServerIdentity) -> io::Result<()> {
        write_json_atomic(&self.root.join("server.json"), id, true)
    }
    pub fn load_config(&self) -> ConfigData {
        read_json(&self.root.join("config.json")).unwrap_or_default()
    }
    pub fn save_config(&self, cfg: &ConfigData) -> io::Result<()> {
        write_json_atomic(&self.root.join("config.json"), cfg, true)
    }

    pub fn load_nodes(&self) -> Vec<NodeRecord> {
        let mut out = Vec::new();
        let Ok(entries) = std::fs::read_dir(self.root.join("nodes")) else { return out };
        for e in entries.flatten() {
            if e.path().extension().is_some_and(|x| x == "json") {
                match read_json::<NodeRecord>(&e.path()) {
                    Some(rec) => out.push(rec),
                    None => tracing::warn!("skipping unparseable node file {:?}", e.path()),
                }
            }
        }
        out.sort_by_key(|r| r.node_id);
        out
    }
    pub fn save_node(&self, rec: &NodeRecord) -> io::Result<()> {
        write_json_atomic(&self.root.join("nodes").join(format!("{}.json", rec.node_id)), rec, false)
    }
    pub fn delete_node(&self, node_id: u64) -> io::Result<()> {
        let p = self.root.join("nodes").join(format!("{node_id}.json"));
        if p.exists() { std::fs::remove_file(p) } else { Ok(()) }
    }
}

/// Strict counterpart of `read_json` for files whose absence and whose
/// corruption mean very different things. Deliberately NOT used by
/// `load_config` (missing -> defaults) or `load_nodes` (one bad node file must
/// not block startup); both of those want the lenient version.
fn read_json_strict<T: serde::de::DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    // Also catches non-UTF-8 (InvalidData), a directory in place of the file,
    // and permission errors — none of which are "not configured yet".
    let data = match std::fs::read_to_string(path) {
        Ok(data) => data,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    serde_json::from_str(&data)
        .map(Some)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{path:?}: {e}")))
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Option<T> {
    let data = std::fs::read_to_string(path).ok()?;
    match serde_json::from_str(&data) {
        Ok(v) => Some(v),
        Err(e) => { tracing::warn!("failed to parse {path:?}: {e}"); None }
    }
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T, secret: bool) -> io::Result<()> {
    let dir = path.parent().unwrap();
    let tmp = dir.join(format!(
        ".{}.tmp-{}", path.file_name().unwrap().to_string_lossy(), std::process::id()));
    {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        if secret { use std::os::unix::fs::OpenOptionsExt; opts.mode(0o600); }
        let file = opts.open(&tmp)?;
        serde_json::to_writer_pretty(&file, value)?;
        file.sync_all()?;
    }
    #[cfg(unix)]
    if secret {
        use std::os::unix::fs::PermissionsExt;
        // OpenOptions mode only applies on create; enforce on every write.
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(&tmp, path)
}

/// Node-server compatible: trim; None/empty -> "HomeAssistant"; hard substring(0,32).
pub fn normalize_fabric_label(label: Option<&str>) -> String {
    let trimmed = label.map(str::trim).unwrap_or("");
    let effective = if trimmed.is_empty() { "HomeAssistant" } else { trimmed };
    effective.chars().take(32).collect()
}

/// ConfigStorage.allocateNodeId: start at next_node_id, skip in-use ids,
/// persist next = candidate + 1 (caller saves config before using the id).
pub fn allocate_node_id(cfg: &mut ConfigData, is_in_use: impl Fn(u64) -> bool) -> u64 {
    let mut candidate = cfg.next_node_id.max(1);
    let start = candidate;
    while is_in_use(candidate) { candidate += 1; }
    if candidate != start {
        tracing::info!("Skipped {} node id(s) from {start} already in use on the fabric, allocated {candidate} instead",
                       candidate - start);
    }
    cfg.next_node_id = candidate + 1;
    candidate
}

/// ConfigStorage credential-id rules ("default" itself is the implicit slot and allowed).
pub fn validate_credential_id(id: &str, existing: impl Iterator<Item = String>) -> Result<(), String> {
    if id.is_empty() {
        return Err("invalid-credential-id: id must be non-empty".into());
    }
    let lower = id.to_ascii_lowercase();
    if lower == "delete" || (lower == "default" && id != "default") {
        return Err(format!("invalid-credential-id: '{id}' is reserved"));
    }
    if lower == "default" { return Ok(()); }
    for other in existing {
        if other.to_ascii_lowercase() == lower && other != id {
            return Err(format!("invalid-credential-id: '{id}' duplicates existing '{other}'"));
        }
    }
    Ok(())
}

pub fn validate_thread_dataset(hex: &str) -> Result<(), String> {
    if hex.is_empty() || hex.len() % 2 != 0 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err("Invalid Thread operational dataset: must be a non-empty hex string with even length (each byte is two hex characters)".into());
    }
    Ok(())
}

/// Node getDateAsString(): LOCAL time, millis + literal "000", no timezone.
pub fn format_node_date(t: std::time::SystemTime) -> String {
    let dt: chrono::DateTime<chrono::Local> = t.into();
    dt.format("%Y-%m-%dT%H:%M:%S%.3f000").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> tempfile::TempDir { tempfile::tempdir().unwrap() }

    #[test]
    fn identity_roundtrip_and_0600() {
        let d = tmp();
        let s = Storage::open(d.path()).unwrap();
        assert!(s.load_identity().unwrap().is_none());
        let id = ServerIdentity {
            fabric_id: 1, vendor_id: 0xFFF1, controller_node_id: 112233,
            compressed_fabric_id: 0xDEADBEEF,
            ca_private_key: vec![1; 32], rcac_tlv: vec![2; 40],
            controller_private_key: vec![3; 32], controller_noc_tlv: vec![4; 40],
            ipk: vec![5; 16],
        };
        s.save_identity(&id).unwrap();
        let back = s.load_identity().unwrap().unwrap();
        assert_eq!(back.compressed_fabric_id, 0xDEADBEEF);
        assert_eq!(back.ipk, vec![5; 16]);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(d.path().join("server.json")).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        // key material is base64 in the file, not JSON arrays
        let raw = std::fs::read_to_string(d.path().join("server.json")).unwrap();
        assert!(!raw.contains("[1,1,1"));
    }

    /// Corruption must never read back as "no identity yet", while config and
    /// node files must stay lenient (defaults / skip-one-file).
    #[test]
    fn identity_load_is_strict_config_and_nodes_stay_lenient() {
        let d = tmp();
        let s = Storage::open(d.path()).unwrap();
        let server = d.path().join("server.json");

        assert!(s.load_identity().unwrap().is_none()); // absent -> None

        std::fs::write(&server, b"{ not json").unwrap();
        assert_eq!(s.load_identity().unwrap_err().kind(), io::ErrorKind::InvalidData);

        std::fs::write(&server, [0x80, 0x81]).unwrap(); // not UTF-8
        assert!(s.load_identity().is_err());

        // Missing field: ServerIdentity has no serde defaults, so this fails
        // the whole parse - and that must surface, not vanish.
        std::fs::write(&server, br#"{"fabric_id":1}"#).unwrap();
        assert!(s.load_identity().is_err());

        // create_identity never clobbers.
        let id = ServerIdentity {
            fabric_id: 1, vendor_id: 0xFFF1, controller_node_id: 112233,
            compressed_fabric_id: 7, ca_private_key: vec![1; 32], rcac_tlv: vec![2; 40],
            controller_private_key: vec![3; 32], controller_noc_tlv: vec![4; 40], ipk: vec![5; 16],
        };
        let before = std::fs::read(&server).unwrap();
        assert_eq!(s.create_identity(&id).unwrap_err().kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&server).unwrap(), before);
        std::fs::remove_file(&server).unwrap();
        s.create_identity(&id).unwrap();
        assert_eq!(s.load_identity().unwrap().unwrap().compressed_fabric_id, 7);

        std::fs::write(d.path().join("config.json"), b"{ not json").unwrap();
        assert_eq!(s.load_config().fabric_label, ConfigData::default().fabric_label);
        std::fs::write(d.path().join("nodes").join("9.json"), b"{ not json").unwrap();
        assert!(s.load_nodes().is_empty());
    }

    #[test]
    fn node_records_roundtrip_and_delete() {
        let d = tmp();
        let s = Storage::open(d.path()).unwrap();
        let mut attributes = serde_json::Map::new();
        attributes.insert("1/6/0".into(), serde_json::json!(true));
        let rec = NodeRecord { node_id: 5, date_commissioned: "x".into(), last_interview: "y".into(),
                               device_fabric_index: 3, addresses: vec!["fe80::1%2".into()], attributes };
        s.save_node(&rec).unwrap();
        let all = s.load_nodes();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].node_id, 5);
        assert_eq!(all[0].device_fabric_index, 3);
        s.delete_node(5).unwrap();
        assert!(s.load_nodes().is_empty());
    }

    #[test]
    fn allocate_skips_in_use_and_advances() {
        let mut cfg = ConfigData::default();
        assert_eq!(cfg.next_node_id, 1);
        let id = allocate_node_id(&mut cfg, |n| n < 3); // 1,2 in use
        assert_eq!(id, 3);
        assert_eq!(cfg.next_node_id, 4);
        let id2 = allocate_node_id(&mut cfg, |_| false);
        assert_eq!(id2, 4);
    }

    #[test]
    fn fabric_label_normalization() {
        assert_eq!(normalize_fabric_label(None), "HomeAssistant");
        assert_eq!(normalize_fabric_label(Some("")), "HomeAssistant");
        assert_eq!(normalize_fabric_label(Some("   ")), "HomeAssistant");
        assert_eq!(normalize_fabric_label(Some("  Casa  ")), "Casa");
        assert_eq!(normalize_fabric_label(Some(&"x".repeat(40))), "x".repeat(32));
    }

    #[test]
    fn credential_id_validation_strings() {
        let existing = || ["Default".to_string(), "garage".to_string()].into_iter();
        assert!(validate_credential_id("default", existing()).is_ok()); // implicit slot always ok
        assert_eq!(validate_credential_id("", existing()).unwrap_err(),
                   "invalid-credential-id: id must be non-empty");
        assert_eq!(validate_credential_id("delete", existing()).unwrap_err(),
                   "invalid-credential-id: 'delete' is reserved");
        assert_eq!(validate_credential_id("GARAGE", existing()).unwrap_err(),
                   "invalid-credential-id: 'GARAGE' duplicates existing 'garage'");
        assert!(validate_credential_id("shed", existing()).is_ok());
    }

    #[test]
    fn thread_dataset_validation() {
        assert!(validate_thread_dataset("0e080000000000010000").is_ok());
        let err = validate_thread_dataset("0e0").unwrap_err();
        assert!(err.starts_with("Invalid Thread operational dataset"));
        assert!(validate_thread_dataset("").is_err());
        assert!(validate_thread_dataset("zz").is_err());
    }

    #[test]
    fn node_date_format() {
        let s = format_node_date(std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(1_755_081_342_123));
        // local time, so only assert the shape: 19 chars datetime + ".SSS000", no Z/offset
        assert_eq!(s.len(), "2026-08-13T10:15:42.123000".len());
        assert!(s.ends_with("000"));
        assert!(!s.contains('Z') && !s.contains('+'));
        let dot = s.rfind('.').unwrap();
        assert_eq!(s.len() - dot, 7); // ".SSS000"
    }
}
