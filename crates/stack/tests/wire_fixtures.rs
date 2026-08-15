//! Golden corpus: attribute TLV -> wire JSON, expectations derived from the
//! matterjs-server converter (Converters.ts) with per-entry citations. Add a
//! fixture here for every future wire regression before fixing it.

use rs_matter::tlv::TLVElement;
use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    name: String,
    #[allow(dead_code)]
    why: String,
    cluster: u32,
    attr: u32,
    tlv: String,
    expect: serde_json::Value,
}

fn unhex(s: &str) -> Vec<u8> {
    let compact: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    (0..compact.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&compact[i..i + 2], 16).expect("fixture hex"))
        .collect()
}

#[test]
fn attribute_conversion_matches_the_golden_corpus() {
    let raw = include_str!("fixtures/attr_values.json");
    let fixtures: Vec<Fixture> = serde_json::from_str(raw).expect("fixture file parses");
    assert!(!fixtures.is_empty());

    let mut failures = Vec::new();
    for f in &fixtures {
        let bytes = unhex(&f.tlv);
        match matter_rs_stack::tlv_json::attr_value_to_json(f.cluster, f.attr, &TLVElement::new(&bytes)) {
            Ok(v) if v == f.expect => {}
            Ok(v) => failures.push(format!("{}: got {v}, expected {}", f.name, f.expect)),
            Err(e) => failures.push(format!("{}: conversion failed: {e}", f.name)),
        }
    }
    assert!(failures.is_empty(), "corpus mismatches:\n{}", failures.join("\n"));
}
