//! matter.js tagged-value codec (StringifyTools.ts, matter.js v0.17.9).
//!
//! Detection is byte-for-byte matter.js's own rule: a value is a tagged
//! document iff it is a STRING that starts with `{"__object__":"` and ends
//! with `}`. An unknown tag is a hard error — a silently-unconverted key is a
//! boot failure at best and a wrong fabric at worst (spec, "Value encoding").

use serde_json::Value;

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum DecodeError {
    #[error("unknown __object__ tag {0:?}")]
    UnknownTag(String),
    #[error("malformed tagged value: {0}")]
    Malformed(String),
    #[error("expected {expected}, got {got}")]
    WrongType { expected: &'static str, got: String },
}

const TAG_PREFIX: &str = "{\"__object__\":\"";

/// Tags whose `__value__` is a decimal string (matter.js maps them via `BigInt(...)`).
const BIGINT_TAGS: &[&str] = &["BigInt", "EventNumber", "FabricId", "NodeId"];
/// Legacy tags whose `__value__` is already a plain JSON number.
const NUMBER_TAGS: &[&str] = &[
    "AttributeId", "CaseAuthenticatedTag", "ClusterId", "CommandId", "DataVersion",
    "DeviceTypeId", "EndpointNumber", "EntryIndex", "EventId", "FabricIndex",
    "FieldId", "GroupId", "VendorId",
];

enum Tagged {
    U64(u64),
    Bytes(Vec<u8>),
    MapEntries(Vec<(Value, Value)>),
    Undefined,
}

/// `None` = not a tagged document (an ordinary value).
fn parse_tagged(v: &Value) -> Option<Result<Tagged, DecodeError>> {
    let s = v.as_str()?;
    if !(s.starts_with(TAG_PREFIX) && s.ends_with('}')) {
        return None;
    }
    Some(parse_tagged_inner(s))
}

fn parse_tagged_inner(s: &str) -> Result<Tagged, DecodeError> {
    let doc: Value = serde_json::from_str(s).map_err(|e| DecodeError::Malformed(e.to_string()))?;
    let tag = doc
        .get("__object__")
        .and_then(Value::as_str)
        .ok_or_else(|| DecodeError::Malformed("__object__ is not a string".into()))?;
    let value = doc.get("__value__");
    if tag == "Undefined" {
        return Ok(Tagged::Undefined);
    }
    if BIGINT_TAGS.contains(&tag) {
        let payload = value
            .and_then(Value::as_str)
            .ok_or_else(|| DecodeError::Malformed(format!("{tag} without a string __value__")))?;
        let n: u64 = payload
            .parse()
            .map_err(|e| DecodeError::Malformed(format!("{tag} {payload:?}: {e}")))?;
        return Ok(Tagged::U64(n));
    }
    if NUMBER_TAGS.contains(&tag) {
        let n = value
            .and_then(Value::as_u64)
            .ok_or_else(|| DecodeError::Malformed(format!("{tag} without a u64 __value__")))?;
        return Ok(Tagged::U64(n));
    }
    if tag == "Uint8Array" {
        let payload = value
            .and_then(Value::as_str)
            .ok_or_else(|| DecodeError::Malformed("Uint8Array without a string __value__".into()))?;
        let bytes = hex::decode(payload)
            .map_err(|e| DecodeError::Malformed(format!("Uint8Array hex: {e}")))?;
        return Ok(Tagged::Bytes(bytes));
    }
    if tag == "Map" {
        // Double-encoded: __value__ is a JSON STRING holding a toJson'd array
        // of [key, value] pairs; the pair members keep their own tags.
        let payload = value
            .and_then(Value::as_str)
            .ok_or_else(|| DecodeError::Malformed("Map without a string __value__".into()))?;
        let inner: Value =
            serde_json::from_str(payload).map_err(|e| DecodeError::Malformed(format!("Map: {e}")))?;
        return entries_from_array(&inner);
    }
    Err(DecodeError::UnknownTag(tag.to_string()))
}

fn entries_from_array(v: &Value) -> Result<Tagged, DecodeError> {
    let Value::Array(items) = v else {
        return Err(DecodeError::Malformed("Map __value__ is not an array".into()));
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item.as_array().map(Vec::as_slice) {
            Some([k, val]) => out.push((k.clone(), val.clone())),
            _ => return Err(DecodeError::Malformed("Map entry is not a [key, value] pair".into())),
        }
    }
    Ok(Tagged::MapEntries(out))
}

fn wrong_type(expected: &'static str, v: &Value) -> DecodeError {
    DecodeError::WrongType { expected, got: short_debug(v) }
}

/// A bounded rendering for error messages (values can be huge blobs).
fn short_debug(v: &Value) -> String {
    let s = v.to_string();
    if s.chars().count() > 80 {
        let head: String = s.chars().take(80).collect();
        format!("{head}…")
    } else {
        s
    }
}

pub fn as_u64(v: &Value) -> Result<u64, DecodeError> {
    match parse_tagged(v) {
        Some(Ok(Tagged::U64(n))) => Ok(n),
        Some(Ok(_)) => Err(wrong_type("u64", v)),
        Some(Err(e)) => Err(e),
        None => v.as_u64().ok_or_else(|| wrong_type("u64", v)),
    }
}

pub fn as_bytes(v: &Value) -> Result<Vec<u8>, DecodeError> {
    match parse_tagged(v) {
        Some(Ok(Tagged::Bytes(b))) => Ok(b),
        Some(Ok(_)) => Err(wrong_type("Uint8Array", v)),
        Some(Err(e)) => Err(e),
        None => Err(wrong_type("Uint8Array", v)),
    }
}

pub fn as_str(v: &Value) -> Result<&str, DecodeError> {
    match parse_tagged(v) {
        Some(Ok(_)) => Err(wrong_type("plain string", v)),
        Some(Err(e)) => Err(e),
        None => v.as_str().ok_or_else(|| wrong_type("plain string", v)),
    }
}

pub fn as_map_entries(v: &Value) -> Result<Vec<(Value, Value)>, DecodeError> {
    match parse_tagged(v) {
        Some(Ok(Tagged::MapEntries(e))) => Ok(e),
        Some(Ok(_)) => Err(wrong_type("Map", v)),
        Some(Err(e)) => Err(e),
        None => match entries_from_array(v) {
            Ok(Tagged::MapEntries(e)) => Ok(e),
            Ok(_) => unreachable!("entries_from_array only builds MapEntries"),
            Err(_) if !v.is_array() => Err(wrong_type("Map", v)),
            Err(e) => Err(e),
        },
    }
}

pub fn is_undefined(v: &Value) -> bool {
    matches!(parse_tagged(v), Some(Ok(Tagged::Undefined)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    // Emit tags exactly as matter.js's toJson does (StringifyTools.ts).
    fn bigint(n: u64) -> Value {
        Value::String(format!("{{\"__object__\":\"BigInt\",\"__value__\":\"{n}\"}}"))
    }
    fn uint8array(hex_str: &str) -> Value {
        Value::String(format!("{{\"__object__\":\"Uint8Array\",\"__value__\":\"{hex_str}\"}}"))
    }

    #[test]
    fn u64_from_plain_number_and_every_bigint_shaped_tag() {
        assert_eq!(as_u64(&json!(65521)), Ok(65521));
        assert_eq!(as_u64(&bigint(112233)), Ok(112233));
        // Legacy tags fromJson maps through BigInt(...): decimal-string payload.
        for tag in ["NodeId", "FabricId", "EventNumber"] {
            let v = Value::String(format!("{{\"__object__\":\"{tag}\",\"__value__\":\"23\"}}"));
            assert_eq!(as_u64(&v), Ok(23), "for tag {tag}");
        }
        // Legacy tags fromJson passes through verbatim: numeric payload.
        let v = Value::String("{\"__object__\":\"VendorId\",\"__value__\":65521}".to_string());
        assert_eq!(as_u64(&v), Ok(65521));
    }

    #[test]
    fn u64_rejects_negatives_floats_and_overflow() {
        assert!(matches!(as_u64(&json!(-1)), Err(DecodeError::WrongType { .. })));
        assert!(matches!(as_u64(&json!(1.5)), Err(DecodeError::WrongType { .. })));
        let v = Value::String(
            "{\"__object__\":\"BigInt\",\"__value__\":\"99999999999999999999\"}".to_string(),
        );
        assert!(matches!(as_u64(&v), Err(DecodeError::Malformed(_))));
        assert!(matches!(as_u64(&json!("112233")), Err(DecodeError::WrongType { .. })));
    }

    #[test]
    fn bytes_accept_both_hex_cases_and_reject_odd_length() {
        assert_eq!(as_bytes(&uint8array("0424fed0b3")), Ok(vec![0x04, 0x24, 0xfe, 0xd0, 0xb3]));
        assert_eq!(as_bytes(&uint8array("ABCD")), Ok(vec![0xab, 0xcd]));
        assert_eq!(as_bytes(&uint8array("")), Ok(vec![]));
        assert!(matches!(as_bytes(&uint8array("abc")), Err(DecodeError::Malformed(_))));
        assert!(matches!(as_bytes(&json!("abcd")), Err(DecodeError::WrongType { .. })));
        assert!(matches!(as_bytes(&bigint(1)), Err(DecodeError::WrongType { .. })));
    }

    #[test]
    fn map_entries_unwrap_the_double_encoding() {
        // matter.js: toJson(map) emits {"__object__":"Map","__value__":<JSON-
        // stringified STRING containing toJson(entries)>}. Build it the same way.
        let inner = serde_json::to_string(&json!([
            [bigint(10), {"discoveryData": {"discoveredAt": 1699999999999u64}}],
            [bigint(23), {"discoveryData": {"discoveredAt": 1700000000000u64}}],
        ]))
        .unwrap();
        let outer = Value::String(format!(
            "{{\"__object__\":\"Map\",\"__value__\":{}}}",
            serde_json::to_string(&inner).unwrap()
        ));
        let entries = as_map_entries(&outer).unwrap();
        assert_eq!(entries.len(), 2);
        // Entries come back RAW: the keys are still tagged, decoded by the caller.
        assert_eq!(as_u64(&entries[0].0), Ok(10));
        assert_eq!(as_u64(&entries[1].0), Ok(23));
        assert_eq!(entries[1].1["discoveryData"]["discoveredAt"], json!(1700000000000u64));

        // A plain array of pairs is also accepted (post-decode shape).
        let plain = json!([[bigint(5), {"x": 1}]]);
        assert_eq!(as_map_entries(&plain).unwrap().len(), 1);
        assert!(matches!(as_map_entries(&json!([[1, 2, 3]])), Err(DecodeError::Malformed(_))));
        assert!(matches!(as_map_entries(&json!(7)), Err(DecodeError::WrongType { .. })));
    }

    #[test]
    fn unknown_tags_are_a_hard_error_never_a_passthrough() {
        // The spec's rule: a silently-unconverted value is a boot failure at
        // best and a wrong fabric at worst. Interval is real-but-unexpected;
        // Foo is hypothetical; both must refuse.
        for tag in ["Interval", "Foo"] {
            let v = Value::String(format!("{{\"__object__\":\"{tag}\",\"__value__\":\"1\"}}"));
            assert_eq!(as_u64(&v), Err(DecodeError::UnknownTag(tag.to_string())), "for {tag}");
            assert_eq!(as_bytes(&v), Err(DecodeError::UnknownTag(tag.to_string())), "for {tag}");
        }
    }

    #[test]
    fn a_string_that_starts_like_a_tag_but_is_not_json_is_malformed() {
        // fromJson would throw on JSON.parse here; mirroring that beats
        // treating it as an ordinary string.
        let v = Value::String("{\"__object__\":\"BigInt\", broken}".to_string());
        assert!(matches!(as_u64(&v), Err(DecodeError::Malformed(_))));
        assert!(matches!(as_str(&v), Err(DecodeError::Malformed(_))));
    }

    #[test]
    fn as_str_takes_plain_strings_only() {
        assert_eq!(as_str(&json!("HomeAssistant")), Ok("HomeAssistant"));
        assert_eq!(as_str(&json!("")), Ok(""));
        assert!(matches!(as_str(&bigint(1)), Err(DecodeError::WrongType { .. })));
        assert!(matches!(as_str(&json!(1)), Err(DecodeError::WrongType { .. })));
    }

    #[test]
    fn undefined_tag_is_recognised() {
        let v = Value::String("{\"__object__\":\"Undefined\"}".to_string());
        assert!(is_undefined(&v));
        assert!(!is_undefined(&json!("x")));
        assert!(!is_undefined(&json!(null)));
        // ...and the typed getters refuse it rather than inventing a value.
        assert!(matches!(as_u64(&v), Err(DecodeError::WrongType { .. })));
    }
}
