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

/// Format version written into every `server.json`.
///
/// It exists so that a *future* field can be added at all. `ServerIdentity` has
/// no container-level serde default by design — a missing field is a hard boot
/// error, pinned by `identity_load_is_strict_config_and_nodes_stay_lenient` —
/// which means adding any field later would be a hard boot failure for every
/// existing install. Nothing branches on this value yet, deliberately: the
/// migration hook has to be on disk *before* it is needed, and a version that
/// changes behaviour today would be a second thing to get right.
pub const IDENTITY_VERSION: u32 = 1;

fn default_identity_version() -> u32 { IDENTITY_VERSION }

#[derive(Clone, Serialize, Deserialize)]
pub struct ServerIdentity {
    /// See [`IDENTITY_VERSION`]. Defaulted (the only defaulted field here) so
    /// that a `server.json` written before this field existed still loads.
    #[serde(default = "default_identity_version")] pub version: u32,
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

/// Hand-written, NOT derived: `ca_private_key`, `controller_private_key` and
/// `ipk` are raw `Vec<u8>`, so a derived `Debug` prints the fabric's trust anchor
/// byte by byte. One future `tracing::debug!("{ready:?}")` on `stack::ReadyInfo`
/// — which is `pub`, holds this, and derives `Debug` — would be enough to put the
/// CA key in a log file. Nothing reaches it today; this makes it impossible
/// rather than merely unattempted.
///
/// The certificates are public, so they are not redacted, just summarised: their
/// bytes are noise in a log and their *length* is what a debug print is for.
impl std::fmt::Debug for ServerIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerIdentity")
            .field("version", &self.version)
            .field("fabric_id", &self.fabric_id)
            .field("vendor_id", &self.vendor_id)
            .field("controller_node_id", &self.controller_node_id)
            .field("compressed_fabric_id", &self.compressed_fabric_id)
            .field("ca_private_key", &format_args!("[redacted; {} bytes]", self.ca_private_key.len()))
            .field("rcac_tlv", &format_args!("[{} bytes]", self.rcac_tlv.len()))
            .field("controller_private_key",
                   &format_args!("[redacted; {} bytes]", self.controller_private_key.len()))
            .field("controller_noc_tlv", &format_args!("[{} bytes]", self.controller_noc_tlv.len()))
            .field("ipk", &format_args!("[redacted; {} bytes]", self.ipk.len()))
            .finish()
    }
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

/// Distinguishes concurrent writers *inside* this process.
///
/// The pid alone is not enough. Two tasks writing config.json at the same time
/// (two WS connections: HA plus a debug client, or a reconnect while the old
/// connection still drains) would open the same `.config.json.tmp-<pid>` with
/// `truncate(true)`, interleave their `to_writer_pretty` output — unbuffered,
/// many small writes straight onto a `&File` — and both rename. The survivor can
/// be invalid JSON, which `load_config` then *silently* reads back as
/// `ConfigData::default()`: no fabric label, no WiFi/Thread credentials, and a
/// reset `next_node_id`, with one `warn!` as the only trace.
///
/// `MatterController::update_config` serializes the config read-modify-write as
/// well; this counter is the belt to that braces, and it also covers two writers
/// aimed at *different* files, which no single lock does.
static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn write_json_atomic<T: Serialize>(path: &Path, value: &T, secret: bool) -> io::Result<()> {
    // No unwraps on the boot path: a path without a parent or a final component
    // cannot be written atomically here, and that has to surface as an error
    // rather than a panic even though every caller passes a well-formed path.
    let (Some(dir), Some(name)) = (path.parent(), path.file_name()) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{path:?} has no parent directory or file name to write atomically"),
        ));
    };
    let seq = TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = dir.join(format!(
        ".{}.tmp-{}-{seq}", name.to_string_lossy(), std::process::id()));
    let result = write_tmp_then_rename(&tmp, path, value, secret);
    if result.is_err() {
        // Without this a read-only or full disk leaves one `.name.tmp-pid-seq`
        // behind per attempt, forever, next to the file it failed to replace.
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

fn write_tmp_then_rename<T: Serialize>(
    tmp: &Path, path: &Path, value: &T, secret: bool,
) -> io::Result<()> {
    {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        if secret { use std::os::unix::fs::OpenOptionsExt; opts.mode(0o600); }
        let file = opts.open(tmp)?;
        serde_json::to_writer_pretty(&file, value)?;
        file.sync_all()?;
    }
    #[cfg(unix)]
    if secret {
        use std::os::unix::fs::PermissionsExt;
        // OpenOptions mode only applies on create; enforce on every write.
        std::fs::set_permissions(tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(tmp, path)
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
            version: IDENTITY_VERSION,
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
            version: IDENTITY_VERSION,
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

    fn sample_identity() -> ServerIdentity {
        ServerIdentity {
            version: IDENTITY_VERSION,
            fabric_id: 1, vendor_id: 0xFFF1, controller_node_id: 112233,
            compressed_fabric_id: 0xDEADBEEF,
            ca_private_key: vec![0xAB; 32], rcac_tlv: vec![0x11; 40],
            controller_private_key: vec![0xCD; 32], controller_noc_tlv: vec![0x22; 40],
            ipk: vec![0xEF; 16],
        }
    }

    /// `ServerIdentity`'s `Debug` is hand-written so that no future `{:?}` — most
    /// plausibly on `stack::ReadyInfo`, which is `pub`, holds this and derives
    /// `Debug` — can put the fabric's trust anchor in a log file.
    #[test]
    fn identity_debug_redacts_every_secret() {
        let printed = format!("{:?}", sample_identity());
        // The three secrets, in either spelling a Debug impl could produce. Runs
        // of three for the hex form: a bare "ab" also occurs inside "fabric_id".
        for byte in [0xABu8, 0xCD, 0xEF] {
            assert!(!printed.contains(&format!("{byte}")), "decimal byte {byte:#x} leaked: {printed}");
            assert!(!printed.to_lowercase().contains(&format!("{byte:02x}").repeat(3)),
                    "hex byte {byte:#x} leaked: {printed}");
        }
        assert_eq!(printed.matches("[redacted; ").count(), 3);
        assert!(printed.contains("[redacted; 32 bytes]")); // the two P-256 keys
        assert!(printed.contains("[redacted; 16 bytes]")); // the IPK
        // Still useful: the non-secret scalars survive.
        assert!(printed.contains("fabric_id: 1"));
        assert!(printed.contains("controller_node_id: 112233"));
        assert!(printed.contains("version: 1"));
    }

    /// The migration trap `version` exists to defuse: `ServerIdentity` has no
    /// container-level serde default (a missing field is a hard error — see
    /// `identity_load_is_strict_config_and_nodes_stay_lenient`), so a field added
    /// later would be a hard boot failure for every existing install. This one is
    /// defaulted, so a `server.json` written before it existed still loads.
    #[test]
    fn identity_version_is_written_and_defaults_when_absent() {
        let d = tmp();
        let s = Storage::open(d.path()).unwrap();
        let server = d.path().join("server.json");
        s.save_identity(&sample_identity()).unwrap();

        // What we write carries it...
        let raw: Value = serde_json::from_str(&std::fs::read_to_string(&server).unwrap()).unwrap();
        assert_eq!(raw["version"], IDENTITY_VERSION);

        // ...and a file predating the field still loads, with everything else intact.
        let mut obj = raw.as_object().unwrap().clone();
        obj.remove("version");
        std::fs::write(&server, serde_json::to_string_pretty(&obj).unwrap()).unwrap();
        let loaded = s.load_identity().unwrap().unwrap();
        assert_eq!(loaded.version, IDENTITY_VERSION);
        assert_eq!(loaded.compressed_fabric_id, 0xDEADBEEF);
        assert_eq!(loaded.ipk, vec![0xEF; 16]);

        // Every OTHER field stays strict: dropping one is still a hard error.
        obj.remove("ipk");
        std::fs::write(&server, serde_json::to_string_pretty(&obj).unwrap()).unwrap();
        assert_eq!(s.load_identity().unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    /// Every writer needs its OWN temp path. Sharing `.config.json.tmp-<pid>`
    /// means two threads both open it with `truncate(true)`, interleave their
    /// unbuffered `to_writer_pretty` output, and both rename — so the file that
    /// survives can be invalid JSON, which `load_config` then *silently* answers
    /// as `ConfigData::default()`: no credentials, no fabric label, reset
    /// `next_node_id`. Threads rather than tasks because threads are the shape
    /// that collides.
    #[test]
    fn concurrent_writers_never_leave_a_torn_config() {
        let d = tmp();
        let s = Storage::open(d.path()).unwrap();
        std::thread::scope(|scope| {
            let s = &s;
            for w in 0..4 {
                scope.spawn(move || {
                    for i in 0..50u64 {
                        let cfg = ConfigData {
                            fabric_label: format!("writer{w}"),
                            next_node_id: 1000 + i,
                            ..Default::default()
                        };
                        s.save_config(&cfg).expect("atomic write must not fail");
                        // Any intermediate state must still parse: a torn file
                        // comes back as the default label instead.
                        let back = s.load_config();
                        assert!(back.fabric_label.starts_with("writer"), "torn config.json: {back:?}");
                        assert!(back.next_node_id >= 1000, "torn config.json: {back:?}");
                    }
                });
            }
        });
    }

    /// A failed write must not leave `.name.tmp-pid-seq` litter next to the file
    /// it did not replace (one per attempt, forever, on a full or read-only disk).
    #[test]
    fn a_failed_atomic_write_removes_its_temp_file() {
        let d = tmp();
        // serde_json rejects a non-string map key, and it does so *after* the
        // temp file exists — exactly the path that used to leak it.
        let unserializable: BTreeMap<(u8, u8), u8> = [((1, 2), 3)].into_iter().collect();
        let path = d.path().join("bad.json");
        assert!(write_json_atomic(&path, &unserializable, false).is_err());
        assert!(!path.exists());
        let litter: Vec<String> = std::fs::read_dir(d.path()).unwrap().flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.contains(".tmp-"))
            .collect();
        assert!(litter.is_empty(), "left temp litter behind: {litter:?}");
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
