//! End-to-end: generate a matter.js store with the reference install's shape
//! (stale snapshot, ~40k-line WAL, multi-admin device caches), migrate it, and
//! read the result back through the server's own Storage. This is the gate
//! that the tool and the server agree on the format.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use matter_rs_controller::storage::Storage;
use matter_rs_migrate::{run, Options};
use matter_rs_stack::migration::{
    derive_operational_ipk, generate_ca, identity_from_preserved_ca, rcac_public_key,
};
use serde_json::{json, Value};

const EPOCH_KEY: [u8; 16] = [0x5a; 16];

// ---- tagged-value emitters (matter.js toJson, StringifyTools.ts) ----

fn bigint(n: u64) -> Value {
    Value::String(format!("{{\"__object__\":\"BigInt\",\"__value__\":\"{n}\"}}"))
}
fn bytes(b: &[u8]) -> Value {
    Value::String(format!("{{\"__object__\":\"Uint8Array\",\"__value__\":\"{}\"}}", hex::encode(b)))
}
fn map_tag(entries: Vec<(Value, Value)>) -> Value {
    let pairs: Vec<Value> = entries.into_iter().map(|(k, v)| json!([k, v])).collect();
    let inner = serde_json::to_string(&Value::Array(pairs)).unwrap();
    Value::String(format!(
        "{{\"__object__\":\"Map\",\"__value__\":{}}}",
        serde_json::to_string(&inner).unwrap()
    ))
}

struct Fixture {
    ca_private_key: Vec<u8>,
    rcac_tlv: Vec<u8>,
    compressed_fabric_id: u64,
    root_public_key: Vec<u8>,
}

fn foreign_root_pk(fill: u8) -> Vec<u8> {
    let mut pk = vec![0x04];
    pk.extend_from_slice(&[fill; 64]);
    pk
}

fn fabric_entry(pk: &[u8], index: u64, label: &str) -> Value {
    json!({
        "rootPublicKey": bytes(pk), "vendorId": 4996, "fabricId": bigint(1),
        "nodeId": bigint(112233), "label": label, "fabricIndex": index,
    })
}

fn commissioned_node(id: u64, addr: Option<&str>, discovered_ms: u64) -> (Value, Value) {
    let mut v = json!({
        "discoveryData": {"discoveredAt": discovered_ms},
        "deviceData": {"basicInformation": {"vendorId": 4996, "productId": 1}},
    });
    if let Some(ip) = addr {
        v["operationalServerAddress"] = json!({"type": "udp", "ip": ip, "port": 5540});
    }
    (bigint(id), v)
}

/// The credentials.fabric object, parameterised by label so both the stale
/// snapshot and the poison-pill WAL lines can reuse it.
fn fabric_field(f: &Fixture, label: &str) -> Value {
    json!({
        "fabricId": bigint(1),
        "nodeId": bigint(112233),
        "rootNodeId": bigint(112233),
        "rootVendorId": 65521,
        "identityProtectionKey": bytes(&EPOCH_KEY),
        "operationalIdentityProtectionKey": bytes(&derive_operational_ipk(&EPOCH_KEY, f.compressed_fabric_id).unwrap()),
        "operationalId": bytes(&f.compressed_fabric_id.to_be_bytes()),
        "label": label,
    })
}

fn gz(data: &[u8]) -> Vec<u8> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    enc.write_all(data).unwrap();
    enc.finish().unwrap()
}

fn upd_line(key: &str, values: Value) -> String {
    json!({"ts": 1700000000000u64, "ops": [{"op": "upd", "key": key, "values": values}]}).to_string()
}

/// Build the whole store under `root`. ~40k WAL lines; runs in well under a
/// second in release-test, a few seconds in debug — acceptable for the gate.
fn build_fixture(root: &Path) -> Fixture {
    let (ca_private_key, rcac_tlv) = generate_ca(1).unwrap();
    let minted =
        identity_from_preserved_ca(&ca_private_key, &rcac_tlv, 1, 0xFFF1, 112233, &EPOCH_KEY).unwrap();
    let f = Fixture {
        root_public_key: rcac_public_key(&rcac_tlv).unwrap(),
        compressed_fabric_id: minted.compressed_fabric_id,
        ca_private_key,
        rcac_tlv,
    };

    // Python-migration leftovers the tool must ignore-and-report.
    std::fs::create_dir_all(root.join("certificates")).unwrap();
    std::fs::create_dir_all(root.join("credentials")).unwrap();
    std::fs::write(root.join("chip_config.ini"), b"[legacy]\n").unwrap();
    std::fs::write(root.join(format!("{:016x}.json", f.compressed_fabric_id)), b"{}").unwrap();

    let ns = root.join("server-1-fff1");
    let wal = ns.join("wal");
    std::fs::create_dir_all(&wal).unwrap();
    std::fs::write(ns.join("driver.json"), br#"{"kind":"wal","type":"kv"}"#).unwrap();

    // Stale snapshot: 3 of 5 nodes, wrong label, commitId (1, 2).
    let snapshot = json!({
        "commitId": {"segment": 1, "offset": 2},
        "ts": 1699000000000u64,
        "data": {
            "credentials": {"fabric": fabric_field(&f, "stale")},
            "certificates": {
                "rootKeyPair": {"privateKey": bytes(&f.ca_private_key), "publicKey": bytes(&f.root_public_key)},
                "rootCertBytes": bytes(&f.rcac_tlv),
            },
            "nodes": {"commissionedNodes": map_tag(vec![
                commissioned_node(10, Some("192.168.1.60"), 1_690_000_000_000),
                commissioned_node(12, None, 1_690_000_100_000),
                commissioned_node(21, Some("192.168.1.61"), 1_690_000_200_000),
            ])},
            "decoy": {"gone": true},
            "decoy.child": {"gone": true},
        },
    });
    std::fs::write(ns.join("snapshot.json.gz"), gz(snapshot.to_string().as_bytes())).unwrap();

    // Segment 1: offsets 0-2 are poison pills already contained in the
    // snapshot — replaying them would set the label to "resurrected".
    let mut seg1: Vec<String> = vec![
        upd_line("credentials", json!({"fabric": fabric_field(&f, "resurrected")})),
        upd_line("credentials", json!({"fabric": fabric_field(&f, "resurrected")})),
        upd_line("credentials", json!({"fabric": fabric_field(&f, "resurrected")})),
    ];
    // Scale realism: ~40k filler attribute-cache updates (reference: 38,723).
    for i in 0..40_000u64 {
        seg1.push(upd_line(
            &format!("nodes.peer{}.endpoints.1.6", 10 + (i % 3)),
            json!({"0": i % 2 == 0}),
        ));
    }
    // The real mutations the snapshot is missing:
    seg1.push(upd_line("nodes", json!({"commissionedNodes": map_tag(vec![
        commissioned_node(10, Some("192.168.1.60"), 1_690_000_000_000),
        commissioned_node(12, None, 1_690_000_100_000),
        commissioned_node(21, Some("192.168.1.61"), 1_690_000_200_000),
        commissioned_node(22, Some("[fe80::9%eth1]"), 1_695_000_000_000),
        commissioned_node(23, Some("fd6a::1"), 1_699_999_999_999),
    ])})));
    seg1.push(upd_line("credentials", json!({"fabric": fabric_field(&f, "")})));
    // A field-del and a subtree-del, so replay exercises every op shape.
    seg1.push(json!({"ts": 1, "ops": [{"op": "del", "key": "decoy", "values": ["gone"]}]}).to_string());
    seg1.push(json!({"ts": 2, "ops": [{"op": "del", "key": "decoy"}]}).to_string());
    // Device fabric tables: node 10 is the spec's multi-admin trap.
    seg1.push(upd_line("nodes.peer10.endpoints.0.62", json!({"1": [
        fabric_entry(&foreign_root_pk(0xBB), 1, "Mijn huis"),
        fabric_entry(&foreign_root_pk(0xCC), 2, ""),
        fabric_entry(&f.root_public_key, 3, "HomeAssistant"),
    ]})));
    seg1.push(upd_line("nodes.peer12.endpoints.0.62", json!({"1": [fabric_entry(&f.root_public_key, 2, "HomeAssistant")]})));
    std::fs::write(wal.join("00000001.jsonl"), seg1.join("\n")).unwrap();

    // Segment 2, gzipped: the remaining device caches. Node 23 gets none.
    let seg2 = [
        upd_line("nodes.peer21.endpoints.0.62", json!({"1": [fabric_entry(&f.root_public_key, 2, "HomeAssistant")]})),
        upd_line("nodes.peer22.endpoints.0.62", json!({"1": [fabric_entry(&f.root_public_key, 4, "HomeAssistant")]})),
    ]
    .join("\n");
    std::fs::write(wal.join("00000002.jsonl.gz"), gz(seg2.as_bytes())).unwrap();

    f
}

/// Recursive (path, bytes) inventory, for proving the source was not touched.
fn dir_inventory(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for e in std::fs::read_dir(&dir).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() { stack.push(p); } else { out.insert(p.clone(), std::fs::read(&p).unwrap()); }
        }
    }
    out
}

#[test]
fn dry_run_checks_everything_and_writes_nothing() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let to = dst.path().join("matter-rs");
    build_fixture(src.path());
    let before = dir_inventory(src.path());

    let report = run(&Options { from: src.path().into(), to: to.clone(), write: false }).unwrap();

    assert!(report.ok(), "checks failed:\n{report}");
    assert_eq!(report.checks.len(), 5);
    assert_eq!(report.namespace, "server-1-fff1");
    assert!(report.wrote.is_none());
    assert!(!to.exists(), "dry run created the destination");
    assert_eq!(dir_inventory(src.path()), before, "the source store was modified");
    // Python leftovers ignored, and said so.
    assert!(report.ignored_python_leftovers.iter().any(|n| n == "chip_config.ini"), "{:?}", report.ignored_python_leftovers);
    // The report text lists the fallback node loudly.
    let text = report.to_string();
    assert!(text.contains("23") && text.to_uppercase().contains("FALLBACK"), "{text}");
}

#[test]
fn write_produces_a_store_the_server_code_reads_back() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let to = dst.path().join("matter-rs");
    let f = build_fixture(src.path());
    let before = dir_inventory(src.path());

    let report = run(&Options { from: src.path().into(), to: to.clone(), write: true }).unwrap();
    assert!(report.ok(), "{report}");
    assert_eq!(report.wrote.as_deref(), Some(to.as_path()));
    assert_eq!(dir_inventory(src.path()), before, "the source store was modified");

    // Read back through the server's own strict loader.
    let storage = Storage::open(&to).unwrap();
    let id = storage.load_identity().unwrap().expect("server.json must exist");
    assert_eq!(id.fabric_id, 1);
    assert_eq!(id.vendor_id, 0xFFF1);
    assert_eq!(id.controller_node_id, 112233);
    assert_eq!(id.compressed_fabric_id, f.compressed_fabric_id);
    assert_eq!(id.ca_private_key, f.ca_private_key);
    assert_eq!(id.rcac_tlv, f.rcac_tlv);
    assert_eq!(id.ipk, EPOCH_KEY.to_vec());
    matter_rs_stack::migration::verify_identity(&id).unwrap();

    let cfg = storage.load_config();
    assert_eq!(cfg.fabric_label, "HomeAssistant"); // empty source label -> default
    assert_eq!(cfg.next_node_id, 24);
    assert!(cfg.wifi_credentials.is_empty() && cfg.thread_datasets.is_empty());

    // WAL replay was mandatory: 22 and 23 exist only in the WAL.
    let nodes = storage.load_nodes();
    assert_eq!(nodes.iter().map(|n| n.node_id).collect::<Vec<_>>(), vec![10, 12, 21, 22, 23]);
    let by_id = |id: u64| nodes.iter().find(|n| n.node_id == id).unwrap();
    assert_eq!(by_id(10).device_fabric_index, 3, "the multi-admin trap");
    assert_eq!(by_id(12).device_fabric_index, 2);
    assert_eq!(by_id(21).device_fabric_index, 2);
    assert_eq!(by_id(22).device_fabric_index, 4);
    assert_eq!(by_id(23).device_fabric_index, 0, "no cache must mean 0, never a guess");
    assert_eq!(by_id(22).addresses, vec!["fe80::9%eth1".to_string()], "brackets must not survive");
    assert_eq!(by_id(23).addresses, vec!["fd6a::1".to_string()]);
    assert!(by_id(12).addresses.is_empty());
    for n in &nodes {
        assert!(n.attributes.is_empty(), "attribute caches must start empty");
        assert!(!n.date_commissioned.is_empty());
        assert_eq!(n.date_commissioned, n.last_interview);
    }
    // The commitId boundary held: the poison-pill label never surfaced.
    assert_ne!(cfg.fabric_label, "resurrected");
}

#[test]
fn a_second_write_refuses_to_overwrite_the_new_fabric() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let to = dst.path().join("matter-rs");
    build_fixture(src.path());

    run(&Options { from: src.path().into(), to: to.clone(), write: true }).unwrap();
    let server_json_before = std::fs::read(to.join("server.json")).unwrap();

    let err = run(&Options { from: src.path().into(), to: to.clone(), write: true }).unwrap_err();
    assert!(err.to_string().contains("server.json"), "{err}");
    assert_eq!(std::fs::read(to.join("server.json")).unwrap(), server_json_before);
}

#[test]
fn a_corrupt_wal_aborts_before_anything_is_written() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let to = dst.path().join("matter-rs");
    build_fixture(src.path());
    // Truncate the last WAL segment mid-line.
    let seg = src.path().join("server-1-fff1/wal/00000001.jsonl");
    let mut bytes = std::fs::read(&seg).unwrap();
    bytes.truncate(bytes.len() - 40);
    std::fs::write(&seg, bytes).unwrap();

    let err = run(&Options { from: src.path().into(), to: to.clone(), write: true }).unwrap_err();
    assert!(err.to_string().contains("00000001.jsonl"), "{err}");
    assert!(!to.exists(), "a failed migration left a partial destination");
}

/// The wrong-IPK store: identityProtectionKey and its operational twin
/// swapped. Everything ELSE is consistent — exactly the "looks correct, fails
/// subtly" fabric check 3 exists to catch. --write must write nothing.
#[test]
fn a_wrong_ipk_choice_fails_check_3_and_writes_nothing() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let to = dst.path().join("matter-rs");
    build_fixture(src.path());

    // Swap the two IPKs in the final WAL state by appending one more commit
    // (segment 3 replays last, so it wins). Read the current fabric object
    // back through the tool's own reader, swap the keys, re-emit.
    let (db, _) = matter_rs_migrate::jsdb::JsDb::open_store(src.path()).unwrap();
    let fabric = db.field("credentials", "fabric").unwrap().clone();
    let epoch = matter_rs_migrate::decode::as_bytes(&fabric["identityProtectionKey"]).unwrap();
    let op = matter_rs_migrate::decode::as_bytes(&fabric["operationalIdentityProtectionKey"]).unwrap();
    let mut swapped = fabric;
    swapped["identityProtectionKey"] = bytes(&op);
    swapped["operationalIdentityProtectionKey"] = bytes(&epoch);
    std::fs::write(
        src.path().join("server-1-fff1/wal/00000003.jsonl"),
        upd_line("credentials", json!({"fabric": swapped})),
    )
    .unwrap();

    let report = run(&Options { from: src.path().into(), to: to.clone(), write: true }).unwrap();
    assert!(!report.ok());
    let ipk = report.checks.iter().find(|c| c.name == "ipk-choice").unwrap();
    assert!(!ipk.passed, "{report}");
    assert!(report.wrote.is_none());
    assert!(!to.exists(), "failed checks must abort before the first write");
}
