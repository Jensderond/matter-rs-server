//! Read-only reader for matter.js's WAL KV store format (matter.js v0.17.9,
//! packages/general/src/storage/wal/). Snapshot-then-replay: the snapshot's
//! `commitId` marks the last (segment, offset) it already includes; only
//! strictly later WAL commits apply. Values stay raw serde_json::Value —
//! tagged strings are `decode`'s business, not ours.
//!
//! One deliberate deviation: matter.js skips malformed WAL lines with a
//! warning; we hard-error naming the line, because a migration that silently
//! drops commits is the failure mode most likely to look like success.

use std::collections::BTreeMap;
use std::io::Read as _;
use std::path::{Path, PathBuf};

use serde_json::Value;

/// Context name -> field map, matter.js's in-memory KV shape.
type Data = BTreeMap<String, serde_json::Map<String, Value>>;

/// A replayed matter.js WAL KV namespace: context name -> field map.
#[derive(Debug)]
pub struct JsDb {
    data: Data,
}

#[derive(Debug, thiserror::Error)]
pub enum JsdbError {
    #[error("cannot read {path}: {source}")]
    Io {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("no server-* namespace under {root} (found: {found:?}); is this a matter.js store?")]
    NoNamespace {
        root: std::path::PathBuf,
        found: Vec<String>,
    },
    #[error("{count} server-* namespaces under {root}: {found:?}; expected exactly one")]
    MultipleNamespaces {
        root: std::path::PathBuf,
        count: usize,
        found: Vec<String>,
    },
    #[error("{path} is not a WAL KV namespace: driver.json says {found}")]
    NotWalKv {
        path: std::path::PathBuf,
        found: String,
    },
    #[error("bad snapshot {path}: {reason}")]
    BadSnapshot { path: std::path::PathBuf, reason: String },
    #[error("bad WAL line {path}:{line_number}: {reason}")]
    BadWalLine {
        path: std::path::PathBuf,
        line_number: usize,
        reason: String,
    },
}

#[derive(serde::Deserialize)]
struct SnapshotFile {
    #[serde(rename = "commitId")]
    commit_id: CommitId,
    data: Data,
}

#[derive(serde::Deserialize, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Debug)]
struct CommitId {
    segment: u64,
    offset: u64,
}

/// Read a file's raw bytes, mapping IO errors to `JsdbError::Io`. Never opens
/// anything for writing.
fn read_bytes(path: &Path) -> Result<Vec<u8>, JsdbError> {
    std::fs::read(path).map_err(|source| JsdbError::Io {
        path: path.to_path_buf(),
        source,
    })
}

/// Decompress gzip bytes into a UTF-8 string, or treat them as already plain.
fn gunzip_to_string(bytes: &[u8], path: &Path) -> Result<String, JsdbError> {
    let mut decoder = flate2::read::GzDecoder::new(bytes);
    let mut out = String::new();
    decoder
        .read_to_string(&mut out)
        .map_err(|source| JsdbError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(out)
}

impl JsDb {
    /// Locate the single `server-*` namespace directory under a matter.js
    /// store root, load it, and return the namespace directory name.
    pub fn open_store(root: &Path) -> Result<(JsDb, String), JsdbError> {
        let entries = std::fs::read_dir(root).map_err(|source| JsdbError::Io {
            path: root.to_path_buf(),
            source,
        })?;
        let mut all_names = Vec::new();
        let mut namespaces = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|source| JsdbError::Io {
                path: root.to_path_buf(),
                source,
            })?;
            let name = entry.file_name().to_string_lossy().into_owned();
            all_names.push(name.clone());
            // A stray file (e.g. a `.bak` leftover) named `server-*` is not a
            // namespace: only a directory can hold a WAL KV store.
            let is_dir = entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false);
            if name.starts_with("server-") && is_dir {
                namespaces.push(name);
            }
        }
        all_names.sort();
        namespaces.sort();
        match namespaces.len() {
            0 => Err(JsdbError::NoNamespace {
                root: root.to_path_buf(),
                found: all_names,
            }),
            1 => {
                let name = namespaces.into_iter().next().unwrap();
                let db = JsDb::open_namespace(&root.join(&name))?;
                Ok((db, name))
            }
            count => Err(JsdbError::MultipleNamespaces {
                root: root.to_path_buf(),
                count,
                found: namespaces,
            }),
        }
    }

    /// Load one namespace directory: driver check, then snapshot, then WAL
    /// replay strictly after the snapshot's commitId.
    pub fn open_namespace(dir: &Path) -> Result<JsDb, JsdbError> {
        let driver_path = dir.join("driver.json");
        let driver_bytes = read_bytes(&driver_path)?;
        let driver: Value = serde_json::from_slice(&driver_bytes).map_err(|e| JsdbError::NotWalKv {
            path: driver_path.clone(),
            found: format!("<unparseable: {e}>"),
        })?;
        let kind = driver.get("kind").and_then(Value::as_str).unwrap_or("");
        let store_type = driver.get("type").and_then(Value::as_str).unwrap_or("");
        if kind != "wal" || store_type != "kv" {
            return Err(JsdbError::NotWalKv {
                path: driver_path,
                found: String::from_utf8_lossy(&driver_bytes).trim().to_string(),
            });
        }

        let (mut data, after) = load_snapshot(dir)?;
        replay_wal(dir, &mut data, after)?;

        Ok(JsDb { data })
    }

    /// Test/back-door constructor: build a store directly from data.
    pub fn from_data(data: Data) -> JsDb {
        JsDb { data }
    }

    pub fn get(&self, context: &str) -> Option<&serde_json::Map<String, Value>> {
        self.data.get(context)
    }

    pub fn field(&self, context: &str, field: &str) -> Option<&Value> {
        self.data.get(context).and_then(|ctx| ctx.get(field))
    }

    pub fn context_keys(&self) -> impl Iterator<Item = &str> {
        self.data.keys().map(String::as_str)
    }
}

/// Load `snapshot.json.gz` / `snapshot.json` (preferring the newer mtime,
/// ties going to `.gz`). Neither file present -> empty data, no boundary.
fn load_snapshot(
    dir: &Path,
) -> Result<(Data, Option<CommitId>), JsdbError> {
    let gz_path = dir.join("snapshot.json.gz");
    let plain_path = dir.join("snapshot.json");
    let gz_meta = std::fs::metadata(&gz_path).ok();
    let plain_meta = std::fs::metadata(&plain_path).ok();

    let chosen: Option<(&Path, bool)> = match (&gz_meta, &plain_meta) {
        (Some(gz_m), Some(plain_m)) => {
            let gz_time = gz_m.modified().ok();
            let plain_time = plain_m.modified().ok();
            if plain_time.is_some() && gz_time.is_some() && plain_time.unwrap() > gz_time.unwrap() {
                Some((&plain_path, false))
            } else {
                Some((&gz_path, true))
            }
        }
        (Some(_), None) => Some((&gz_path, true)),
        (None, Some(_)) => Some((&plain_path, false)),
        (None, None) => None,
    };

    let Some((path, is_gz)) = chosen else {
        return Ok((BTreeMap::new(), None));
    };

    let bytes = read_bytes(path)?;
    let text = if is_gz {
        gunzip_to_string(&bytes, path)?
    } else {
        String::from_utf8_lossy(&bytes).into_owned()
    };

    let snapshot: SnapshotFile = serde_json::from_str(&text).map_err(|e| JsdbError::BadSnapshot {
        path: path.to_path_buf(),
        reason: e.to_string(),
    })?;

    Ok((snapshot.data, Some(snapshot.commit_id)))
}

/// List `wal/` segments: filename `^[0-9a-f]{8}\.jsonl(\.gz)?$` (case
/// insensitive), keyed by segment number, `.gz` overwriting a plain twin.
fn list_wal_segments(wal_dir: &Path) -> Result<BTreeMap<u64, PathBuf>, JsdbError> {
    let mut segments: BTreeMap<u64, PathBuf> = BTreeMap::new();
    let entries = match std::fs::read_dir(wal_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(segments),
        Err(source) => {
            return Err(JsdbError::Io {
                path: wal_dir.to_path_buf(),
                source,
            })
        }
    };

    for entry in entries {
        let entry = entry.map_err(|source| JsdbError::Io {
            path: wal_dir.to_path_buf(),
            source,
        })?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(segment) = parse_segment_filename(&name) {
            let is_gz = name.to_ascii_lowercase().ends_with(".gz");
            let path = entry.path();
            if is_gz {
                // .gz always wins, regardless of insertion order.
                segments.insert(segment, path);
            } else {
                // A plain segment never displaces an already-chosen entry.
                segments.entry(segment).or_insert(path);
            }
        }
    }
    Ok(segments)
}

/// Parse `^[0-9a-f]{8}\.jsonl(\.gz)?$` case-insensitively; returns the
/// segment number.
fn parse_segment_filename(name: &str) -> Option<u64> {
    let lower = name.to_ascii_lowercase();
    let hex_part = lower.strip_suffix(".jsonl.gz").or_else(|| lower.strip_suffix(".jsonl"))?;
    if hex_part.len() != 8 || !hex_part.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    u64::from_str_radix(hex_part, 16).ok()
}

/// Replay all WAL segments in ascending numeric order, strictly after `after`.
fn replay_wal(
    dir: &Path,
    data: &mut Data,
    after: Option<CommitId>,
) -> Result<(), JsdbError> {
    let wal_dir = dir.join("wal");
    let segments = list_wal_segments(&wal_dir)?;

    for (segment_number, path) in segments {
        if let Some(after) = after {
            if segment_number < after.segment {
                continue;
            }
        }
        replay_segment(&path, segment_number, data, after)?;
    }
    Ok(())
}

/// Replay one WAL segment file, applying commits with offset strictly after
/// `after` (when `after` is in this segment) or all commits (otherwise).
fn replay_segment(
    path: &Path,
    segment_number: u64,
    data: &mut Data,
    after: Option<CommitId>,
) -> Result<(), JsdbError> {
    let is_gz = path
        .extension()
        .map(|ext| ext.eq_ignore_ascii_case("gz"))
        .unwrap_or(false);
    let bytes = read_bytes(path)?;
    let text = if is_gz {
        gunzip_to_string(&bytes, path)?
    } else {
        String::from_utf8_lossy(&bytes).into_owned()
    };

    for (offset, line) in text.split('\n').enumerate() {
        let commit_id = CommitId {
            segment: segment_number,
            offset: offset as u64,
        };
        if line.trim().is_empty() {
            continue;
        }
        if let Some(after) = after {
            if commit_id <= after {
                continue;
            }
        }
        apply_line(path, offset + 1, line, data)?;
    }
    Ok(())
}

/// Parse and apply one WAL line's ops. `line_number` is 1-based.
fn apply_line(
    path: &Path,
    line_number: usize,
    line: &str,
    data: &mut Data,
) -> Result<(), JsdbError> {
    let value: Value = serde_json::from_str(line).map_err(|e| JsdbError::BadWalLine {
        path: path.to_path_buf(),
        line_number,
        reason: e.to_string(),
    })?;

    let ops: &Vec<Value> = match &value {
        Value::Array(ops) => ops,
        Value::Object(obj) => match obj.get("ops") {
            Some(Value::Array(ops)) => ops,
            _ => {
                return Err(JsdbError::BadWalLine {
                    path: path.to_path_buf(),
                    line_number,
                    reason: "commit object missing an \"ops\" array".to_string(),
                })
            }
        },
        _ => {
            return Err(JsdbError::BadWalLine {
                path: path.to_path_buf(),
                line_number,
                reason: "WAL line is neither an array nor an object".to_string(),
            })
        }
    };

    for op in ops {
        apply_op(path, line_number, op, data)?;
    }
    Ok(())
}

/// Apply one op to `data`. This mirrors matter.js's `applyCommit` verbatim.
fn apply_op(
    path: &Path,
    line_number: usize,
    op: &Value,
    data: &mut Data,
) -> Result<(), JsdbError> {
    let bad = |reason: &str| JsdbError::BadWalLine {
        path: path.to_path_buf(),
        line_number,
        reason: reason.to_string(),
    };

    let obj = op.as_object().ok_or_else(|| bad("op is not an object"))?;
    let kind = obj.get("op").and_then(Value::as_str).ok_or_else(|| bad("op missing \"op\" kind"))?;
    let key = obj.get("key").and_then(Value::as_str).ok_or_else(|| bad("op missing \"key\""))?;

    match kind {
        "upd" => {
            let values = obj
                .get("values")
                .and_then(Value::as_object)
                .ok_or_else(|| bad("upd op's \"values\" is not an object"))?;
            let ctx = data.entry(key.to_string()).or_default();
            for (field, value) in values {
                ctx.insert(field.clone(), value.clone());
            }
        }
        "del" => match obj.get("values") {
            Some(Value::Array(fields)) => {
                let mut field_names = Vec::with_capacity(fields.len());
                for f in fields {
                    field_names.push(f.as_str().ok_or_else(|| bad("del op's \"values\" is not an array of strings"))?);
                }
                if let Some(ctx) = data.get_mut(key) {
                    for f in field_names {
                        ctx.remove(f);
                    }
                }
            }
            Some(_) => return Err(bad("del op's \"values\" is not an array of strings")),
            None => {
                if key.is_empty() {
                    data.clear();
                } else {
                    data.remove(key);
                    let prefix = format!("{key}.");
                    data.retain(|k, _| !k.starts_with(&prefix));
                }
            }
        },
        _ => return Err(bad("unknown op kind")),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::io::Write as _;
    use std::path::Path;

    fn write_driver(ns: &Path) {
        std::fs::create_dir_all(ns).unwrap();
        std::fs::write(ns.join("driver.json"), br#"{"kind":"wal","type":"kv"}"#).unwrap();
    }

    /// Write `snapshot.json.gz` with the given commitId and data.
    fn write_snapshot(ns: &Path, segment: u64, offset: u64, data: Value) {
        let doc = json!({"commitId": {"segment": segment, "offset": offset}, "ts": 1700000000000u64, "data": data});
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        gz.write_all(doc.to_string().as_bytes()).unwrap();
        std::fs::write(ns.join("snapshot.json.gz"), gz.finish().unwrap()).unwrap();
    }

    /// Write one WAL segment file from raw lines (joined with \n).
    fn write_wal(ns: &Path, filename: &str, lines: &[&str]) {
        let wal = ns.join("wal");
        std::fs::create_dir_all(&wal).unwrap();
        let body = lines.join("\n");
        if filename.ends_with(".gz") {
            let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            gz.write_all(body.as_bytes()).unwrap();
            std::fs::write(wal.join(filename), gz.finish().unwrap()).unwrap();
        } else {
            std::fs::write(wal.join(filename), body).unwrap();
        }
    }

    fn upd(key: &str, values: Value) -> String {
        json!({"ts": 1700000000001u64, "ops": [{"op": "upd", "key": key, "values": values}]}).to_string()
    }

    #[test]
    fn snapshot_alone_loads_when_there_is_no_wal() {
        let d = tempfile::tempdir().unwrap();
        write_driver(d.path());
        write_snapshot(d.path(), 1, 0, json!({"credentials": {"fabric": {"label": "x"}}}));
        let db = JsDb::open_namespace(d.path()).unwrap();
        assert_eq!(db.field("credentials", "fabric").unwrap()["label"], json!("x"));
        assert!(db.get("nope").is_none());
    }

    #[test]
    fn wal_replay_merges_updates_and_creates_new_contexts() {
        let d = tempfile::tempdir().unwrap();
        write_driver(d.path());
        write_snapshot(d.path(), 0, 0, json!({"a": {"kept": 1, "overwritten": 1}}));
        write_wal(d.path(), "00000001.jsonl", &[
            &upd("a", json!({"overwritten": 2, "added": 3})),
            &upd("brand.new", json!({"x": true})),
        ]);
        let db = JsDb::open_namespace(d.path()).unwrap();
        let a = db.get("a").unwrap();
        assert_eq!(a["kept"], json!(1));
        assert_eq!(a["overwritten"], json!(2));
        assert_eq!(a["added"], json!(3));
        assert_eq!(db.field("brand.new", "x"), Some(&json!(true)));
    }

    /// The commitId boundary: lines at or before the snapshot's commitId are
    /// already IN the snapshot; re-applying them must not resurrect state.
    /// Blank lines still consume an offset (WalReader.ts counts, then skips).
    #[test]
    fn replay_starts_strictly_after_the_snapshot_commit_id() {
        let d = tempfile::tempdir().unwrap();
        write_driver(d.path());
        write_snapshot(d.path(), 1, 1, json!({"a": {}}));
        write_wal(d.path(), "00000001.jsonl", &[
            &upd("a", json!({"resurrected": true})), // offset 0: <= (1,1), skip
            "",                                       // offset 1: blank, but COUNTS
            &upd("a", json!({"fresh": true})),        // offset 2: > (1,1), apply
        ]);
        let db = JsDb::open_namespace(d.path()).unwrap();
        let a = db.get("a").unwrap();
        assert!(a.get("resurrected").is_none(), "re-applied a pre-snapshot commit");
        assert_eq!(a["fresh"], json!(true), "blank lines must still count toward offsets");
    }

    #[test]
    fn all_three_del_shapes_apply_exactly_as_matter_js_does() {
        let d = tempfile::tempdir().unwrap();
        write_driver(d.path());
        write_snapshot(d.path(), 0, 0, json!({
            "a": {"f1": 1, "f2": 2},
            "nodes": {"commissionedNodes": 1},
            "nodes.peer1": {"x": 1},
            "nodes.peer1.endpoints.0.62": {"1": 1},
            "nodesibling": {"survives": true},
        }));
        write_wal(d.path(), "00000001.jsonl", &[
            // field delete
            &json!({"ts": 1, "ops": [{"op": "del", "key": "a", "values": ["f1"]}]}).to_string(),
            // subtree delete: "nodes" AND every "nodes.*" context — but NOT "nodesibling"
            &json!({"ts": 2, "ops": [{"op": "del", "key": "nodes"}]}).to_string(),
        ]);
        let db = JsDb::open_namespace(d.path()).unwrap();
        assert!(db.field("a", "f1").is_none());
        assert_eq!(db.field("a", "f2"), Some(&json!(2)));
        assert!(db.get("nodes").is_none());
        assert!(db.get("nodes.peer1").is_none());
        assert!(db.get("nodes.peer1.endpoints.0.62").is_none());
        assert_eq!(db.field("nodesibling", "survives"), Some(&json!(true)));
    }

    #[test]
    fn del_with_empty_key_and_no_values_clears_everything() {
        let d = tempfile::tempdir().unwrap();
        write_driver(d.path());
        write_snapshot(d.path(), 0, 0, json!({"a": {"x": 1}, "b": {"y": 2}}));
        write_wal(d.path(), "00000001.jsonl", &[
            &json!({"ts": 1, "ops": [{"op": "del", "key": ""}]}).to_string(),
            &upd("after", json!({"z": 3})),
        ]);
        let db = JsDb::open_namespace(d.path()).unwrap();
        assert!(db.get("a").is_none());
        assert!(db.get("b").is_none());
        assert_eq!(db.field("after", "z"), Some(&json!(3)));
    }

    #[test]
    fn segments_replay_in_numeric_order_and_gz_is_preferred() {
        let d = tempfile::tempdir().unwrap();
        write_driver(d.path());
        write_snapshot(d.path(), 0, 0, json!({}));
        write_wal(d.path(), "00000002.jsonl", &[&upd("a", json!({"v": "seg2"}))]);
        write_wal(d.path(), "00000001.jsonl", &[&upd("a", json!({"v": "seg1"}))]);
        // A .gz twin of segment 1 with different content: the .gz must win.
        write_wal(d.path(), "00000001.jsonl.gz", &[&upd("a", json!({"v": "seg1-gz", "gz": true}))]);
        let db = JsDb::open_namespace(d.path()).unwrap();
        assert_eq!(db.field("a", "v"), Some(&json!("seg2")), "segment 2 must replay after 1");
        assert_eq!(db.field("a", "gz"), Some(&json!(true)), "the .gz segment variant must be preferred");
    }

    #[test]
    fn a_legacy_bare_array_line_is_a_commit() {
        let d = tempfile::tempdir().unwrap();
        write_driver(d.path());
        write_snapshot(d.path(), 0, 0, json!({}));
        write_wal(d.path(), "00000001.jsonl", &[
            &json!([{"op": "upd", "key": "a", "values": {"legacy": true}}]).to_string(),
        ]);
        let db = JsDb::open_namespace(d.path()).unwrap();
        assert_eq!(db.field("a", "legacy"), Some(&json!(true)));
    }

    /// The spec's rule, deliberately stricter than matter.js (which warns and
    /// skips): a corrupt or truncated WAL is a hard error naming the line.
    #[test]
    fn a_malformed_wal_line_is_a_hard_error_naming_the_line() {
        let d = tempfile::tempdir().unwrap();
        write_driver(d.path());
        write_snapshot(d.path(), 0, 0, json!({}));
        write_wal(d.path(), "00000001.jsonl", &[
            &upd("a", json!({"x": 1})),
            "{ truncated",
        ]);
        let err = JsDb::open_namespace(d.path()).unwrap_err();
        match err {
            JsdbError::BadWalLine { line_number, ref path, .. } => {
                assert_eq!(line_number, 2);
                assert!(path.ends_with("wal/00000001.jsonl"));
            }
            other => panic!("expected BadWalLine, got {other}"),
        }
        // Unknown op kinds and non-object lines are the same hard error.
        write_wal(d.path(), "00000001.jsonl", &[
            &json!({"ts": 1, "ops": [{"op": "zap", "key": "a"}]}).to_string(),
        ]);
        assert!(matches!(JsDb::open_namespace(d.path()), Err(JsdbError::BadWalLine { line_number: 1, .. })));
    }

    #[test]
    fn missing_snapshot_replays_the_whole_wal_and_missing_wal_is_fine() {
        let d = tempfile::tempdir().unwrap();
        write_driver(d.path());
        write_wal(d.path(), "00000001.jsonl", &[&upd("a", json!({"x": 1}))]);
        let db = JsDb::open_namespace(d.path()).unwrap();
        assert_eq!(db.field("a", "x"), Some(&json!(1)));

        let d2 = tempfile::tempdir().unwrap();
        write_driver(d2.path());
        write_snapshot(d2.path(), 0, 0, json!({"only": {"snap": 1}}));
        let db2 = JsDb::open_namespace(d2.path()).unwrap();
        assert_eq!(db2.field("only", "snap"), Some(&json!(1)));
    }

    #[test]
    fn driver_json_must_declare_a_wal_kv_store() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path()).unwrap();
        std::fs::write(d.path().join("driver.json"), br#"{"kind":"jsonfile","type":"kv"}"#).unwrap();
        assert!(matches!(JsDb::open_namespace(d.path()), Err(JsdbError::NotWalKv { .. })));
    }

    /// Store-root discovery: exactly one server-* namespace; python-migration
    /// leftovers (chip_*.ini, certificates/, credentials/) must not confuse it.
    #[test]
    fn open_store_finds_exactly_one_server_namespace() {
        let root = tempfile::tempdir().unwrap();
        let ns = root.path().join("server-1-fff1");
        write_driver(&ns);
        write_snapshot(&ns, 0, 0, json!({"a": {"x": 1}}));
        // Distractors: other namespaces and python leftovers.
        std::fs::create_dir_all(root.path().join("client")).unwrap();
        std::fs::create_dir_all(root.path().join("certificates")).unwrap();
        std::fs::write(root.path().join("chip_config.ini"), b"").unwrap();
        // Stray FILES named `server-*` must not count as namespaces — only a
        // directory can hold a WAL KV store.
        std::fs::write(root.path().join("server-9-dead.bak"), b"").unwrap();
        std::fs::write(root.path().join("server-2-beef"), b"").unwrap();

        let (db, name) = JsDb::open_store(root.path()).unwrap();
        assert_eq!(name, "server-1-fff1");
        assert_eq!(db.field("a", "x"), Some(&json!(1)));

        // None found → NoNamespace listing what IS there.
        let empty = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(empty.path().join("client")).unwrap();
        match JsDb::open_store(empty.path()).unwrap_err() {
            JsdbError::NoNamespace { found, .. } => assert!(found.contains(&"client".to_string())),
            other => panic!("expected NoNamespace, got {other}"),
        }

        // Two found → refuse loudly rather than pick one.
        let ns2 = root.path().join("server-2-1234");
        write_driver(&ns2);
        assert!(matches!(JsDb::open_store(root.path()), Err(JsdbError::MultipleNamespaces { count: 2, .. })));
    }
}
