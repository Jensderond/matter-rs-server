//! TLV <-> JSON, in the two shapes the WS wire uses: *tag-based* (attribute
//! values: decimal field-id keys) and *name-based* (command responses and
//! event data: camelCase keys from the `gen` IDL tables).
//!
//! Octet strings are base64 (STANDARD, padded) and epoch fields are Unix on the
//! JSON side but Matter-relative in TLV, so both directions shift them.

use std::collections::BTreeMap;

use base64::Engine as _;
use serde_json::{Map, Value};

use matter_rs_gen::{Cluster, Field, Struct as GenStruct};
use rs_matter::error::{Error, ErrorCode};
use rs_matter::tlv::{TLVElement, TLVTag, TLVValue, TLVWrite};

pub const MATTER_EPOCH_OFFSET_S: u64 = 946_684_800; // 2000-01-01 - 1970-01-01
pub const MATTER_EPOCH_OFFSET_US: u64 = 946_684_800_000_000;

/// Nesting the read side accepts before giving up. Real IM payloads nest a
/// handful of levels; malformed TLV arrives straight off the network and a
/// blown stack is a SIGABRT, not an error we could map to a wire response.
const MAX_DEPTH: u8 = 32;

/// What `gen` knows about the TLV slot a JSON value is being written into:
/// the IDL type name, whether it is a list, and the cluster whose `structs`
/// resolve a struct-typed name.
#[derive(Debug, Clone, Copy)]
pub struct TypeHint<'a> {
    pub ty: &'a str,
    pub is_list: bool,
    pub cluster: Option<&'static Cluster>,
}

/// One of the IDL's base ("primitive") types, as opposed to a named
/// enum/bitmap/struct type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Base {
    Bool,
    /// Width in bytes, used to range-check the JSON number.
    Unsigned(u8),
    Signed(u8),
    Single,
    Double,
    Utf8,
    Octets,
    EpochS,
    EpochUs,
}

const BASE_TYPES: &[(&str, Base)] = &[
    ("boolean", Base::Bool),
    ("single", Base::Single),
    ("double", Base::Double),
    ("char_string", Base::Utf8), ("long_char_string", Base::Utf8),
    ("octet_string", Base::Octets), ("long_octet_string", Base::Octets),
    ("epoch_s", Base::EpochS),
    ("epoch_us", Base::EpochUs),
    ("int8u", Base::Unsigned(1)), ("enum8", Base::Unsigned(1)), ("bitmap8", Base::Unsigned(1)),
    ("percent", Base::Unsigned(1)), ("fabric_idx", Base::Unsigned(1)),
    ("action_id", Base::Unsigned(1)), ("priority", Base::Unsigned(1)), ("status", Base::Unsigned(1)),
    ("int16u", Base::Unsigned(2)), ("enum16", Base::Unsigned(2)), ("bitmap16", Base::Unsigned(2)),
    ("percent100ths", Base::Unsigned(2)), ("group_id", Base::Unsigned(2)),
    ("endpoint_no", Base::Unsigned(2)), ("vendor_id", Base::Unsigned(2)), ("entry_idx", Base::Unsigned(2)),
    ("int24u", Base::Unsigned(3)),
    ("int32u", Base::Unsigned(4)), ("bitmap32", Base::Unsigned(4)), ("cluster_id", Base::Unsigned(4)),
    ("attrib_id", Base::Unsigned(4)), ("field_id", Base::Unsigned(4)), ("command_id", Base::Unsigned(4)),
    ("event_id", Base::Unsigned(4)), ("devtype_id", Base::Unsigned(4)), ("trans_id", Base::Unsigned(4)),
    ("data_ver", Base::Unsigned(4)), ("elapsed_s", Base::Unsigned(4)),
    ("int40u", Base::Unsigned(5)), ("int48u", Base::Unsigned(6)), ("int56u", Base::Unsigned(7)),
    ("int64u", Base::Unsigned(8)), ("bitmap64", Base::Unsigned(8)), ("node_id", Base::Unsigned(8)),
    ("fabric_id", Base::Unsigned(8)), ("subject_id", Base::Unsigned(8)), ("event_no", Base::Unsigned(8)),
    ("systime_us", Base::Unsigned(8)), ("systime_ms", Base::Unsigned(8)), ("posix_ms", Base::Unsigned(8)),
    ("money", Base::Unsigned(8)),
    ("int8s", Base::Signed(1)),
    ("int16s", Base::Signed(2)), ("temperature", Base::Signed(2)),
    ("int24s", Base::Signed(3)), ("int32s", Base::Signed(4)),
    ("int40s", Base::Signed(5)), ("int48s", Base::Signed(6)), ("int56s", Base::Signed(7)),
    ("int64s", Base::Signed(8)), ("amperage_ma", Base::Signed(8)), ("voltage_mv", Base::Signed(8)),
    ("power_mw", Base::Signed(8)), ("power_mva", Base::Signed(8)), ("power_mvar", Base::Signed(8)),
    ("energy_mwh", Base::Signed(8)), ("energy_mvah", Base::Signed(8)), ("energy_mvarh", Base::Signed(8)),
];

/// Base type names are matched case-insensitively: the vendored V1.6.0.0 IDL
/// spells five of them in caps (`EPOCH_US`, `OCTET_STRING`, `CHAR_STRING`,
/// `INT64U`, `INT8U` — e.g. MediaPlayback's `StateChanged` event) and `gen`
/// carries the spelling through verbatim, so `==` would silently treat those
/// fields as named types. Only the *lookup* folds case: `ty` itself keeps its
/// original casing for `Cluster::find_struct`, which is case-sensitive because
/// `Struct.name` is the IDL's own spelling.
fn base_type(ty: &str) -> Option<Base> {
    BASE_TYPES.iter().find(|(name, _)| name.eq_ignore_ascii_case(ty)).map(|(_, base)| *base)
}

fn invalid() -> Error {
    // Task 15 maps this onto the InvalidArguments wire error.
    ErrorCode::InvalidData.into()
}

pub fn tlv_to_json(elem: &TLVElement) -> Result<Value, Error> {
    tlv_to_json_at(elem, 0)
}

/// A UTF-8 string leaf, decoded lossily (invalid sequences become U+FFFD).
///
/// `Ok(None)` when the element is not a UTF-8 string at all. Must run before
/// `TLVElement::value()`, which hard-fails invalid UTF-8 with TLVTypeMismatch
/// (`rs-matter-ref/rs-matter/src/tlv/read.rs:229-240`) — the failure that cost
/// node 12 its 0/52/0 report. matter.js decodes lossily, and JSON cannot carry
/// the raw bytes regardless, so replacement is the only wire-compatible shape.
/// `octets()` (not `str()`, which is octet-strings-only per `is_str`) returns
/// the raw payload of any variable-size element.
fn lossy_utf8(elem: &TLVElement) -> Result<Option<Value>, Error> {
    Ok(if elem.control()?.value_type.is_utf8() {
        Some(Value::from(String::from_utf8_lossy(elem.octets()?).into_owned()))
    } else {
        None
    })
}

fn tlv_to_json_at(elem: &TLVElement, depth: u8) -> Result<Value, Error> {
    if depth > MAX_DEPTH {
        return Err(invalid());
    }
    if let Some(s) = lossy_utf8(elem)? {
        return Ok(s);
    }
    Ok(match elem.value()? {
        TLVValue::S8(v) => Value::from(v), TLVValue::S16(v) => Value::from(v),
        TLVValue::S32(v) => Value::from(v), TLVValue::S64(v) => Value::from(v),
        TLVValue::U8(v) => Value::from(v), TLVValue::U16(v) => Value::from(v),
        TLVValue::U32(v) => Value::from(v), TLVValue::U64(v) => Value::from(v),
        TLVValue::False => Value::from(false), TLVValue::True => Value::from(true),
        TLVValue::F32(v) => Value::from(v), TLVValue::F64(v) => Value::from(v),
        TLVValue::Utf8l(s) | TLVValue::Utf16l(s) | TLVValue::Utf32l(s) | TLVValue::Utf64l(s) => Value::from(s),
        TLVValue::Str8l(b) | TLVValue::Str16l(b) | TLVValue::Str32l(b) | TLVValue::Str64l(b) =>
            Value::from(base64::engine::general_purpose::STANDARD.encode(b)),
        TLVValue::Null => Value::Null,
        TLVValue::Struct => {
            let mut obj = Map::new();
            for child in elem.container()?.iter() {
                let child = child?;
                match child.tag()? {
                    TLVTag::Context(n) => { obj.insert(n.to_string(), tlv_to_json_at(&child, depth + 1)?); }
                    other => tracing::debug!("skipping non-context struct member tag {other:?}"),
                }
            }
            Value::Object(obj)
        }
        TLVValue::Array | TLVValue::List => {
            let mut arr = Vec::new();
            for child in elem.container()?.iter() {
                arr.push(tlv_to_json_at(&child?, depth + 1)?);
            }
            Value::Array(arr)
        }
        TLVValue::EndCnt => return Err(ErrorCode::InvalidData.into()),
    })
}

/// Attribute values stay tag-based on the wire; the only type-driven step is
/// the epoch shift, and only at the top level.
///
/// Nested epoch fields deliberately pass through raw: matter.js's own wire
/// format carries Matter-epoch values at every depth, not Unix. The JS
/// layer's *internal* values are Unix
/// (`matterjs-server/packages/ws-controller/test/TimeSyncCommandsTest.ts:61`
/// — `utcTime` equals `BigInt(NOW_MS) * 1000n`, Unix µs; `:64`/`:173-176`
/// pass a positive `MATTER_EPOCH_OFFSET_US` for an entry required to be "at
/// the Matter epoch", which is what pins the constant's sign), but
/// `Converters.ts:394-399` *subtracts* that offset when converting
/// matter -> WS — i.e. it re-encodes back to Matter epoch before the value
/// ever reaches the wire. So passing nested fields through raw here matches
/// Node's actual wire bytes exactly.
///
/// The top-level shift to Unix, by contrast, is pre-existing plan-2
/// behaviour: a known divergence from Node's wire (which stays Matter-epoch
/// even at the top level) whose parity question is open for the maintainer
/// to rule on — see README's "Accepted parity gaps" #7. This function does
/// not change that; it only stops shifting nested fields, which Task 5 had
/// wrongly started doing.
pub fn attr_value_to_json(cluster: u32, attr: u32, elem: &TLVElement) -> Result<Value, Error> {
    let ty = matter_rs_gen::cluster(cluster)
        .and_then(|c| c.attr(attr))
        .map_or("", |a| a.ty);
    apply_epoch(ty, tlv_to_json(elem)?)
}

pub fn tlv_to_json_named(elem: &TLVElement, fields: &[Field], cluster: &Cluster) -> Result<Value, Error> {
    tlv_to_json_named_at(elem, fields, cluster, 0)
}

fn tlv_to_json_named_at(elem: &TLVElement, fields: &[Field], cluster: &Cluster, depth: u8)
                        -> Result<Value, Error> {
    if depth > MAX_DEPTH {
        return Err(invalid());
    }
    if let Some(s) = lossy_utf8(elem)? {
        return Ok(s);
    }
    if !matches!(elem.value()?, TLVValue::Struct) {
        // Only structs carry field ids, so there is nothing to name.
        return tlv_to_json_at(elem, depth);
    }
    let mut obj = Map::new();
    for child in elem.container()?.iter() {
        let child = child?;
        match child.tag()? {
            TLVTag::Context(n) => match fields.iter().find(|f| f.code == n as u32) {
                Some(f) => { obj.insert(f.name.to_string(), named_field_to_json(&child, f, cluster, depth + 1)?); }
                // Field ids the vendored IDL revision doesn't know still have to
                // reach the client, so they keep the tag-based key.
                None => { obj.insert(n.to_string(), tlv_to_json_at(&child, depth + 1)?); }
            },
            other => tracing::debug!("skipping non-context struct member tag {other:?}"),
        }
    }
    Ok(Value::Object(obj))
}

fn named_field_to_json(elem: &TLVElement, f: &Field, cluster: &Cluster, depth: u8) -> Result<Value, Error> {
    if let Some(s) = lossy_utf8(elem)? {
        return Ok(s);
    }
    let Some(nested) = cluster.find_struct(f.ty) else {
        return apply_epoch(f.ty, tlv_to_json_at(elem, depth)?);
    };
    match elem.value()? {
        TLVValue::Struct => tlv_to_json_named_at(elem, nested.fields, cluster, depth),
        TLVValue::Array | TLVValue::List => {
            let mut arr = Vec::new();
            for child in elem.container()?.iter() {
                arr.push(tlv_to_json_named_at(&child?, nested.fields, cluster, depth + 1)?);
            }
            Ok(Value::Array(arr))
        }
        _ => tlv_to_json_at(elem, depth), // nullable struct field, or a device that sent something else
    }
}

/// Matter epochs count from 2000-01-01; the wire JSON uses Unix.
pub(crate) fn apply_epoch(ty: &str, v: Value) -> Result<Value, Error> {
    let offset = match base_type(ty) {
        Some(Base::EpochS) => MATTER_EPOCH_OFFSET_S,
        Some(Base::EpochUs) => MATTER_EPOCH_OFFSET_US,
        _ => return Ok(v),
    };
    // A nullable epoch that came back Null - or a device that sent something
    // else entirely - passes through untouched.
    let Some(matter) = v.as_u64() else { return Ok(v) };
    // Overflow must not fall back to `v`: that would emit a Matter-relative
    // value as if it were already Unix, and the reverse trip would "succeed".
    matter.checked_add(offset).map(Value::from).ok_or_else(invalid)
}

/// Deliberately unguarded against deep nesting, unlike the read side: JSON
/// reaches us through `serde_json::from_str` (`crates/server/src/ws.rs:116`),
/// which rejects anything nested deeper than 128 before we see it.
pub fn write_json<W: TLVWrite>(w: &mut W, tag: &TLVTag, v: &Value, hint: Option<TypeHint<'_>>) -> Result<(), Error> {
    match hint {
        Some(h) if h.is_list => {
            if v.is_null() {
                return w.null(tag);
            }
            let items = v.as_array().ok_or_else(invalid)?;
            w.start_array(tag)?;
            for item in items {
                write_json(w, &TLVTag::Anonymous, item, Some(TypeHint { is_list: false, ..h }))?;
            }
            w.end_container()
        }
        Some(h) => write_hinted(w, tag, v, &h),
        None => write_heuristic(w, tag, v),
    }
}

pub fn write_json_named<W: TLVWrite>(w: &mut W, tag: &TLVTag, obj: &Map<String, Value>,
                                     fields: &[Field], cluster: &'static Cluster) -> Result<(), Error> {
    emit_struct(w, tag, obj, |key| {
        // Unknown key -> Err, which Task 15 reports as InvalidArguments.
        let f = fields.iter().find(|f| f.name.eq_ignore_ascii_case(key)).ok_or_else(invalid)?;
        let ctx = u8::try_from(f.code).map_err(|_| invalid())?;
        Ok((ctx, Some(TypeHint { ty: f.ty, is_list: f.is_list, cluster: Some(cluster) })))
    })
}

/// The one place struct members get emitted: `resolve` maps a JSON key onto its
/// context tag plus whatever `gen` knows about that slot.
fn emit_struct<'h, W, R>(w: &mut W, tag: &TLVTag, obj: &Map<String, Value>, mut resolve: R)
                         -> Result<(), Error>
where
    W: TLVWrite,
    R: FnMut(&str) -> Result<(u8, Option<TypeHint<'h>>), Error>,
{
    // BTreeMap: TLV struct members are canonically ordered by ascending tag.
    let mut sorted: BTreeMap<u8, (Option<TypeHint<'h>>, &Value)> = BTreeMap::new();
    for (key, v) in obj {
        let (ctx, hint) = resolve(key)?;
        // Distinct keys can resolve to one tag ("1"/"01"/"+1", or "level"/"Level"
        // case-insensitively). Silently keeping the last would drop a member the
        // caller asked for, and an *unknown* key is already an error.
        if sorted.insert(ctx, (hint, v)).is_some() {
            return Err(invalid());
        }
    }
    w.start_struct(tag)?;
    for (ctx, (hint, v)) in sorted {
        write_json(w, &TLVTag::Context(ctx), v, hint)?;
    }
    w.end_container()
}

fn write_hinted<W: TLVWrite>(w: &mut W, tag: &TLVTag, v: &Value, h: &TypeHint<'_>) -> Result<(), Error> {
    if v.is_null() {
        return w.null(tag); // `gen` doesn't track nullability; trust the caller
    }
    match base_type(h.ty) {
        Some(base) => write_base(w, tag, v, base),
        // Not a base type, so either a struct we can resolve in the cluster, or
        // a named enum/bitmap - the IDL declares those base types elsewhere, so
        // `gen` can't tell us the width and the heuristics take over.
        None => match h.cluster.and_then(|c| c.find_struct(h.ty).map(|nested| (nested, c))) {
            Some((nested, cluster)) => write_hinted_struct(w, tag, v, nested, cluster),
            None => write_heuristic(w, tag, v),
        },
    }
}

fn write_base<W: TLVWrite>(w: &mut W, tag: &TLVTag, v: &Value, base: Base) -> Result<(), Error> {
    match base {
        Base::Bool => w.bool(tag, v.as_bool().ok_or_else(invalid)?),
        Base::Single => {
            let n = as_f64(v)?;
            let narrowed = n as f32;
            // `as` saturates to +-inf, which would come back as JSON null; the
            // integer paths reject out-of-range values, so this one does too.
            if !narrowed.is_finite() {
                return Err(invalid());
            }
            w.f32(tag, narrowed)
        }
        Base::Double => w.f64(tag, as_f64(v)?),
        Base::Utf8 => w.utf8(tag, v.as_str().ok_or_else(invalid)?),
        Base::Octets => {
            let raw = base64::engine::general_purpose::STANDARD
                .decode(v.as_str().ok_or_else(invalid)?).map_err(|_| invalid())?;
            w.str(tag, &raw)
        }
        Base::EpochS => {
            let matter = to_matter_epoch(v, MATTER_EPOCH_OFFSET_S)?;
            w.u32(tag, u32::try_from(matter).map_err(|_| invalid())?)
        }
        Base::EpochUs => w.u64(tag, to_matter_epoch(v, MATTER_EPOCH_OFFSET_US)?),
        Base::Unsigned(bytes) => write_unsigned(w, tag, v, bytes),
        Base::Signed(bytes) => write_signed(w, tag, v, bytes),
    }
}

fn write_hinted_struct<W: TLVWrite>(w: &mut W, tag: &TLVTag, v: &Value, nested: &'static GenStruct,
                                    cluster: &'static Cluster) -> Result<(), Error> {
    let obj = v.as_object().ok_or_else(invalid)?;
    // A nested struct arrives tag-based when it came from an attribute-shaped
    // value and name-based when it came from a command payload; accept both.
    if obj.keys().all(|k| k.parse::<u8>().is_ok()) {
        write_tag_based_struct(w, tag, obj, Some((nested.fields, cluster)))
    } else {
        write_json_named(w, tag, obj, nested.fields, cluster)
    }
}

fn write_heuristic<W: TLVWrite>(w: &mut W, tag: &TLVTag, v: &Value) -> Result<(), Error> {
    match v {
        Value::Null => w.null(tag),
        Value::Bool(b) => w.bool(tag, *b),
        // rs-matter's u64/i64 writers already narrow to the minimal TLV width.
        Value::Number(n) => match (n.as_u64(), n.as_i64()) {
            (Some(u), _) => w.u64(tag, u),
            (None, Some(i)) => w.i64(tag, i),
            _ => w.f64(tag, n.as_f64().ok_or_else(invalid)?),
        },
        Value::String(s) => w.utf8(tag, s),
        Value::Array(items) => {
            w.start_array(tag)?;
            for item in items {
                write_json(w, &TLVTag::Anonymous, item, None)?;
            }
            w.end_container()
        }
        Value::Object(obj) => write_tag_based_struct(w, tag, obj, None),
    }
}

fn write_tag_based_struct<W: TLVWrite>(w: &mut W, tag: &TLVTag, obj: &Map<String, Value>,
                                       nested: Option<(&[Field], &'static Cluster)>)
                                       -> Result<(), Error> {
    emit_struct(w, tag, obj, |key| {
        let ctx: u8 = key.parse().map_err(|_| invalid())?;
        let hint = nested.and_then(|(fields, cluster)| {
            fields.iter().find(|f| f.code == ctx as u32)
                .map(|f| TypeHint { ty: f.ty, is_list: f.is_list, cluster: Some(cluster) })
        });
        Ok((ctx, hint))
    })
}

fn write_unsigned<W: TLVWrite>(w: &mut W, tag: &TLVTag, v: &Value, bytes: u8) -> Result<(), Error> {
    let n = v.as_u64().ok_or_else(invalid)?;
    match bytes {
        1 => w.u8(tag, u8::try_from(n).map_err(|_| invalid())?),
        2 => w.u16(tag, u16::try_from(n).map_err(|_| invalid())?),
        4 => w.u32(tag, u32::try_from(n).map_err(|_| invalid())?),
        // int24u/int40u/int48u/int56u have no TLV type of their own: range-check
        // the odd width here, then let the u64 writer narrow to the smallest
        // standard width that fits.
        3 | 5 | 6 | 7 => {
            if n >= 1u64 << (bytes as u32 * 8) {
                return Err(invalid());
            }
            w.u64(tag, n)
        }
        _ => w.u64(tag, n),
    }
}

fn write_signed<W: TLVWrite>(w: &mut W, tag: &TLVTag, v: &Value, bytes: u8) -> Result<(), Error> {
    let n = v.as_i64().ok_or_else(invalid)?;
    match bytes {
        1 => w.i8(tag, i8::try_from(n).map_err(|_| invalid())?),
        2 => w.i16(tag, i16::try_from(n).map_err(|_| invalid())?),
        4 => w.i32(tag, i32::try_from(n).map_err(|_| invalid())?),
        3 | 5 | 6 | 7 => {
            let limit = 1i64 << (bytes as u32 * 8 - 1);
            if !(-limit..limit).contains(&n) {
                return Err(invalid());
            }
            w.i64(tag, n)
        }
        _ => w.i64(tag, n),
    }
}

/// Inverse of `apply_epoch`. A value below the offset can't be a Unix
/// timestamp, so reject it rather than wrap into a bogus Matter epoch.
fn to_matter_epoch(v: &Value, offset: u64) -> Result<u64, Error> {
    v.as_u64().ok_or_else(invalid)?.checked_sub(offset).ok_or_else(invalid)
}

fn as_f64(v: &Value) -> Result<f64, Error> {
    v.as_f64().ok_or_else(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rs_matter::tlv::{TLVElement, TLVTag, TLVWrite};
    use rs_matter::utils::storage::WriteBuf;
    use serde_json::json;

    fn build(f: impl FnOnce(&mut WriteBuf<'_>)) -> Vec<u8> {
        let mut buf = [0u8; 256];
        let mut wb = WriteBuf::new(&mut buf);
        f(&mut wb);
        wb.as_slice().to_vec()
    }

    /// Write `v` under `hint` and hand back the TLV, so tests can assert on the
    /// encoding rather than only on a JSON round trip.
    fn write(v: &Value, hint: Option<TypeHint<'_>>) -> Result<Vec<u8>, Error> {
        let mut buf = [0u8; 256];
        let mut wb = WriteBuf::new(&mut buf);
        write_json(&mut wb, &TLVTag::Anonymous, v, hint)?;
        Ok(wb.as_slice().to_vec())
    }

    fn hint(ty: &str) -> Option<TypeHint<'_>> {
        Some(TypeHint { ty, is_list: false, cluster: None })
    }

    #[test]
    fn struct_to_tag_based_object() {
        let bytes = build(|w| {
            w.start_struct(&TLVTag::Anonymous).unwrap();
            w.u16(&TLVTag::Context(0), 22).unwrap();
            w.utf8(&TLVTag::Context(1), "hi").unwrap();
            w.bool(&TLVTag::Context(2), true).unwrap();
            w.null(&TLVTag::Context(3)).unwrap();
            w.end_container().unwrap();
        });
        let v = tlv_to_json(&TLVElement::new(&bytes)).unwrap();
        assert_eq!(v, json!({"0": 22, "1": "hi", "2": true, "3": null}));
    }

    #[test]
    fn array_of_structs_and_octets() {
        let bytes = build(|w| {
            w.start_array(&TLVTag::Anonymous).unwrap();
            w.start_struct(&TLVTag::Anonymous).unwrap();
            w.u8(&TLVTag::Context(0), 14).unwrap();
            w.end_container().unwrap();
            w.str(&TLVTag::Anonymous, &[0xDE, 0xAD]).unwrap();
            w.end_container().unwrap();
        });
        let v = tlv_to_json(&TLVElement::new(&bytes)).unwrap();
        assert_eq!(v, json!([{"0": 14}, "3q0="])); // base64(0xDE 0xAD)
    }

    #[test]
    fn signed_and_large_unsigned() {
        let bytes = build(|w| {
            w.start_struct(&TLVTag::Anonymous).unwrap();
            w.i32(&TLVTag::Context(0), -5).unwrap();
            w.u64(&TLVTag::Context(1), u64::MAX).unwrap();
            w.end_container().unwrap();
        });
        let v = tlv_to_json(&TLVElement::new(&bytes)).unwrap();
        assert_eq!(v["0"], json!(-5));
        assert_eq!(v["1"], json!(u64::MAX)); // stays a full-precision number
    }

    /// `levels` well-formed nested containers. Kept valid TLV on purpose: the
    /// only thing that can make the walk fail is the depth cap.
    fn nested(levels: usize, ctx: Option<u8>) -> Vec<u8> {
        let mut buf = [0u8; 4096];
        let mut wb = WriteBuf::new(&mut buf);
        for _ in 0..levels {
            match ctx {
                Some(n) => wb.start_struct(&TLVTag::Context(n)).unwrap(),
                None => wb.start_array(&TLVTag::Anonymous).unwrap(),
            }
        }
        for _ in 0..levels {
            wb.end_container().unwrap();
        }
        wb.as_slice().to_vec()
    }

    #[test]
    fn deeply_nested_tlv_is_rejected_not_aborted() {
        // 400 nested containers are 800 bytes - inside a single 1583-byte Matter
        // packet - and without a cap the recursive walk overflows the stack:
        // SIGABRT, no unwind, nothing to map to a wire error.
        assert!(tlv_to_json(&TLVElement::new(&nested(MAX_DEPTH as usize, None))).is_ok());
        assert!(tlv_to_json(&TLVElement::new(&nested(400, None))).is_err());
    }

    #[test]
    fn deeply_nested_named_tlv_is_rejected_not_aborted() {
        // Same cap along the named path, which walks structs field by field.
        let cluster = matter_rs_gen::cluster(63).unwrap();
        let input = cluster.find_struct("KeySetWriteRequest").unwrap();
        let shallow = nested(MAX_DEPTH as usize, Some(0));
        assert!(tlv_to_json_named(&TLVElement::new(&shallow), input.fields, cluster).is_ok());
        let deep = nested(400, Some(0));
        assert!(tlv_to_json_named(&TLVElement::new(&deep), input.fields, cluster).is_err());
    }

    #[test]
    fn named_conversion_uses_gen_fields() {
        // OperationalCredentials NOCResponse: statusCode=0, fabricIndex=1
        let cluster = matter_rs_gen::cluster(62).unwrap();
        let resp = cluster.find_struct("NOCResponse").unwrap();
        let bytes = build(|w| {
            w.start_struct(&TLVTag::Anonymous).unwrap();
            w.u8(&TLVTag::Context(0), 0).unwrap();
            w.u8(&TLVTag::Context(1), 3).unwrap();
            w.end_container().unwrap();
        });
        let v = tlv_to_json_named(&TLVElement::new(&bytes), resp.fields, cluster).unwrap();
        assert_eq!(v, json!({"statusCode": 0, "fabricIndex": 3}));
    }

    #[test]
    fn named_conversion_falls_back_to_numeric_key() {
        // A field id the IDL doesn't know (vendor extension / newer revision)
        // must survive as its decimal key rather than being dropped.
        let cluster = matter_rs_gen::cluster(62).unwrap();
        let resp = cluster.find_struct("NOCResponse").unwrap();
        let bytes = build(|w| {
            w.start_struct(&TLVTag::Anonymous).unwrap();
            w.u8(&TLVTag::Context(0), 0).unwrap();
            w.u8(&TLVTag::Context(9), 7).unwrap();
            w.end_container().unwrap();
        });
        let v = tlv_to_json_named(&TLVElement::new(&bytes), resp.fields, cluster).unwrap();
        assert_eq!(v, json!({"statusCode": 0, "9": 7}));
    }

    #[test]
    fn named_conversion_recurses_into_nested_struct() {
        // GroupKeyManagement KeySetReadResponse { groupKeySet: GroupKeySetStruct }
        // exercises nested naming + base64 octets + epoch_us inside the nesting.
        let cluster = matter_rs_gen::cluster(63).unwrap();
        let resp = cluster.find_struct("KeySetReadResponse").unwrap();
        let bytes = build(|w| {
            w.start_struct(&TLVTag::Anonymous).unwrap();
            w.start_struct(&TLVTag::Context(0)).unwrap();
            w.u16(&TLVTag::Context(0), 42).unwrap();
            w.str(&TLVTag::Context(2), &[0xDE, 0xAD]).unwrap();
            w.u64(&TLVTag::Context(3), 100).unwrap();
            w.end_container().unwrap();
            w.end_container().unwrap();
        });
        let v = tlv_to_json_named(&TLVElement::new(&bytes), resp.fields, cluster).unwrap();
        assert_eq!(
            v,
            json!({"groupKeySet": {
                "groupKeySetID": 42,
                "epochKey0": "3q0=",
                "epochStartTime0": 100u64 + MATTER_EPOCH_OFFSET_US,
            }})
        );
    }

    #[test]
    fn uppercase_idl_type_names_are_recognized_on_read() {
        // MediaPlayback StateChanged declares `EPOCH_US startTime = 1` in caps,
        // and gen keeps that spelling; it must convert exactly like `epoch_us`
        // (its lowercase sibling GroupKeySetStruct.epochStartTime0 does).
        let cluster = matter_rs_gen::cluster(1286).unwrap();
        let ev = cluster.event(0).unwrap();
        assert_eq!(ev.fields.iter().find(|f| f.code == 1).unwrap().ty, "EPOCH_US");
        let bytes = build(|w| {
            w.start_struct(&TLVTag::Anonymous).unwrap();
            w.u64(&TLVTag::Context(1), 100).unwrap();
            w.end_container().unwrap();
        });
        let v = tlv_to_json_named(&TLVElement::new(&bytes), ev.fields, cluster).unwrap();
        assert_eq!(v, json!({"startTime": 100u64 + MATTER_EPOCH_OFFSET_US}));
    }

    #[test]
    fn uppercase_idl_type_names_are_recognized_on_write() {
        // Same five caps spellings on the write side: OCTET_STRING must base64-
        // decode (not write the text through), INT8U must range-check.
        assert_eq!(write(&json!("3q0="), hint("OCTET_STRING")).unwrap(),
                   write(&json!("3q0="), hint("octet_string")).unwrap());
        assert_eq!(write(&json!("hi"), hint("CHAR_STRING")).unwrap(),
                   write(&json!("hi"), hint("char_string")).unwrap());
        assert_eq!(write(&json!(100u64 + MATTER_EPOCH_OFFSET_US), hint("EPOCH_US")).unwrap(),
                   write(&json!(100u64 + MATTER_EPOCH_OFFSET_US), hint("epoch_us")).unwrap());
        assert!(write(&json!(300), hint("INT8U")).is_err());
        assert!(write(&json!(300), hint("int8u")).is_err());
    }

    #[test]
    fn write_named_payload_roundtrip() {
        // LevelControl MoveToLevelRequest { level: int8u = 0, transitionTime = 1, ... }
        let cluster = matter_rs_gen::cluster(8).unwrap();
        let input = cluster.find_struct("MoveToLevelRequest").unwrap();
        let payload = json!({"level": 100, "transitionTime": null, "optionsMask": 0, "optionsOverride": 0});
        let mut buf = [0u8; 128];
        let mut wb = rs_matter::utils::storage::WriteBuf::new(&mut buf);
        write_json_named(&mut wb, &TLVTag::Anonymous,
                         payload.as_object().unwrap(), input.fields, cluster).unwrap();
        let back = tlv_to_json(&TLVElement::new(wb.as_slice())).unwrap();
        assert_eq!(back["0"], 100);
        assert_eq!(back["1"], serde_json::Value::Null);
        // Whole object, so a stray extra member can't slip through.
        assert_eq!(back, json!({"0": 100, "1": null, "2": 0, "3": 0}));
    }

    #[test]
    fn write_named_unknown_field_errors() {
        let cluster = matter_rs_gen::cluster(8).unwrap();
        let input = cluster.find_struct("MoveToLevelRequest").unwrap();
        let payload = json!({"nope": 1});
        let mut buf = [0u8; 64];
        let mut wb = rs_matter::utils::storage::WriteBuf::new(&mut buf);
        assert!(write_json_named(&mut wb, &TLVTag::Anonymous,
                                 payload.as_object().unwrap(), input.fields, cluster).is_err());
    }

    #[test]
    fn write_named_resolves_field_names_case_insensitively() {
        let cluster = matter_rs_gen::cluster(8).unwrap();
        let input = cluster.find_struct("MoveToLevelRequest").unwrap();
        let payload = json!({"Level": 5});
        let mut buf = [0u8; 64];
        let mut wb = rs_matter::utils::storage::WriteBuf::new(&mut buf);
        write_json_named(&mut wb, &TLVTag::Anonymous,
                         payload.as_object().unwrap(), input.fields, cluster).unwrap();
        let back = tlv_to_json(&TLVElement::new(wb.as_slice())).unwrap();
        assert_eq!(back, json!({"0": 5}));
    }

    #[test]
    fn write_named_duplicate_keys_error() {
        // "level" and "Level" are the same field: dropping one silently would
        // encode a payload the caller didn't ask for.
        let cluster = matter_rs_gen::cluster(8).unwrap();
        let input = cluster.find_struct("MoveToLevelRequest").unwrap();
        let payload = json!({"level": 1, "Level": 2});
        let mut buf = [0u8; 64];
        let mut wb = rs_matter::utils::storage::WriteBuf::new(&mut buf);
        assert!(write_json_named(&mut wb, &TLVTag::Anonymous,
                                 payload.as_object().unwrap(), input.fields, cluster).is_err());
    }

    #[test]
    fn tag_based_duplicate_keys_error() {
        // "1", "01" and "+1" all parse to context tag 1.
        assert!(write(&json!({"1": 11, "+1": 22}), None).is_err());
        assert!(write(&json!({"1": 11, "01": 33}), None).is_err());
    }

    #[test]
    fn write_named_nested_struct_roundtrip() {
        // Nested struct members may arrive name-based (matching the payload's own
        // convention) or tag-based; both must land on the same TLV.
        let cluster = matter_rs_gen::cluster(63).unwrap();
        let input = cluster.find_struct("KeySetWriteRequest").unwrap();
        let resp = cluster.find_struct("KeySetReadResponse").unwrap();
        let unix_us = 100u64 + MATTER_EPOCH_OFFSET_US;
        let named = json!({"groupKeySet": {
            "groupKeySetID": 42, "groupKeySecurityPolicy": 1,
            "epochKey0": "3q0=", "epochStartTime0": unix_us, "epochKey1": null,
        }});
        let tagged = json!({"groupKeySet": {
            "0": 42, "1": 1, "2": "3q0=", "3": unix_us, "4": null,
        }});
        let encode = |payload: &Value| {
            let mut buf = [0u8; 128];
            let mut wb = rs_matter::utils::storage::WriteBuf::new(&mut buf);
            write_json_named(&mut wb, &TLVTag::Anonymous,
                             payload.as_object().unwrap(), input.fields, cluster).unwrap();
            wb.as_slice().to_vec()
        };
        let from_named = encode(&named);
        assert_eq!(from_named, encode(&tagged));
        // epoch_us survives the round trip: written matter-relative, read back Unix.
        let back = tlv_to_json_named(&TLVElement::new(&from_named), resp.fields, cluster).unwrap();
        assert_eq!(back["groupKeySet"]["epochStartTime0"], json!(unix_us));
        assert_eq!(back["groupKeySet"]["epochKey0"], json!("3q0="));
        assert_eq!(back["groupKeySet"]["epochKey1"], Value::Null);
    }

    #[test]
    fn nested_struct_hint_without_cluster_errors() {
        // No cluster means the struct type can't be resolved, so name-based keys
        // have nothing to resolve against and must not be silently mangled.
        let named = json!({"groupKeySetID": 42});
        assert!(write(&named, hint("GroupKeySetStruct")).is_err());
        // Tag-based keys still encode: they need no field table.
        let tagged = json!({"0": 42});
        assert_eq!(write(&tagged, hint("GroupKeySetStruct")).unwrap(),
                   write(&tagged, None).unwrap());
    }

    #[test]
    fn write_tag_based_with_octet_hint_roundtrip() {
        let mut buf = [0u8; 64];
        let mut wb = rs_matter::utils::storage::WriteBuf::new(&mut buf);
        write_json(&mut wb, &TLVTag::Anonymous, &json!("3q0="),
                   Some(TypeHint { ty: "octet_string", is_list: false, cluster: None })).unwrap();
        let elem = TLVElement::new(wb.as_slice());
        assert_eq!(elem.octets().unwrap(), &[0xDE, 0xAD]);
    }

    #[test]
    fn write_list_hint_wraps_array() {
        let mut buf = [0u8; 64];
        let mut wb = rs_matter::utils::storage::WriteBuf::new(&mut buf);
        write_json(&mut wb, &TLVTag::Anonymous, &json!([1, 2, 3]),
                   Some(TypeHint { ty: "int16u", is_list: true, cluster: None })).unwrap();
        let back = tlv_to_json(&TLVElement::new(wb.as_slice())).unwrap();
        assert_eq!(back, json!([1, 2, 3]));
    }

    #[test]
    fn write_hint_rejects_out_of_range_value() {
        let mut buf = [0u8; 64];
        let mut wb = rs_matter::utils::storage::WriteBuf::new(&mut buf);
        assert!(write_json(&mut wb, &TLVTag::Anonymous, &json!(300),
                           Some(TypeHint { ty: "int8u", is_list: false, cluster: None })).is_err());
    }

    #[test]
    fn odd_width_integers_stay_signed_and_range_checked() {
        // int40s/48s/56s have no TLV type of their own. They must still encode as
        // *signed* (a strict device rejects U8 for a signed field) and reject
        // values outside their declared width.
        let bytes = write(&json!(5), hint("int40s")).unwrap();
        assert_eq!(TLVElement::new(&bytes).value().unwrap(), TLVValue::S8(5));
        assert!(write(&json!(1i64 << 39), hint("int40s")).is_err());
        assert!(write(&json!(-(1i64 << 39) - 1), hint("int40s")).is_err());
        assert!(write(&json!((1i64 << 39) - 1), hint("int40s")).is_ok());
        // int24u/int24s are range-checked to their real width, not to 32 bits.
        assert!(write(&json!(1u64 << 24), hint("int24u")).is_err());
        assert!(write(&json!((1u64 << 24) - 1), hint("int24u")).is_ok());
        assert!(write(&json!(1i64 << 23), hint("int24s")).is_err());
    }

    #[test]
    fn float_hints_roundtrip_and_reject_overflow() {
        let single = write(&json!(1.5), hint("single")).unwrap();
        assert_eq!(tlv_to_json(&TLVElement::new(&single)).unwrap(), json!(1.5));
        let double = write(&json!(1e300), hint("double")).unwrap();
        assert_eq!(tlv_to_json(&TLVElement::new(&double)).unwrap(), json!(1e300));
        // `1e300 as f32` saturates to inf, which reads back as JSON null.
        assert!(write(&json!(1e300), hint("single")).is_err());
    }

    #[test]
    fn write_without_hint_uses_heuristics() {
        let mut buf = [0u8; 128];
        let mut wb = rs_matter::utils::storage::WriteBuf::new(&mut buf);
        let v = json!({"0": true, "1": -7, "2": 70000, "3": "hi", "4": [1, null], "5": 1.5});
        write_json(&mut wb, &TLVTag::Anonymous, &v, None).unwrap();
        let back = tlv_to_json(&TLVElement::new(wb.as_slice())).unwrap();
        assert_eq!(back, v);
    }

    #[test]
    fn tag_based_object_sorts_context_tags() {
        let mut buf = [0u8; 64];
        let mut wb = rs_matter::utils::storage::WriteBuf::new(&mut buf);
        write_json(&mut wb, &TLVTag::Anonymous, &json!({"2": 2, "0": 0, "10": 10}), None).unwrap();
        let back = tlv_to_json(&TLVElement::new(wb.as_slice())).unwrap();
        // JSON object key order isn't asserted; the TLV encodes 0,2,10 in order
        // and parses back completely.
        assert_eq!(back, json!({"0": 0, "2": 2, "10": 10}));
    }

    #[test]
    fn tag_based_object_emits_ascending_context_tags() {
        // The sortedness above is only observable in the TLV byte stream, so pin
        // it directly: struct members must be in ascending context-tag order.
        let mut buf = [0u8; 64];
        let mut wb = rs_matter::utils::storage::WriteBuf::new(&mut buf);
        write_json(&mut wb, &TLVTag::Anonymous, &json!({"10": 10, "2": 2, "0": 0}), None).unwrap();
        let elem = TLVElement::new(wb.as_slice());
        let tags: Vec<u8> = elem.structure().unwrap().iter()
            .map(|c| c.unwrap().ctx().unwrap())
            .collect();
        assert_eq!(tags, vec![0, 2, 10]);
    }

    #[test]
    fn epoch_seconds_attribute_converts_to_unix() {
        let bytes = build(|w| { w.u32(&TLVTag::Anonymous, 100).unwrap(); });
        let v = apply_epoch("epoch_s", tlv_to_json(&TLVElement::new(&bytes)).unwrap()).unwrap();
        assert_eq!(v, json!(100u64 + MATTER_EPOCH_OFFSET_S));
    }

    #[test]
    fn epoch_seconds_attribute_via_attr_table() {
        // SmokeCoAlarm (92) expiryDate (12) is typed epoch_s in the IDL.
        assert_eq!(matter_rs_gen::cluster(92).unwrap().attr(12).unwrap().ty, "epoch_s");
        let bytes = build(|w| { w.u32(&TLVTag::Anonymous, 100).unwrap(); });
        let v = attr_value_to_json(92, 12, &TLVElement::new(&bytes)).unwrap();
        assert_eq!(v, json!(100u64 + MATTER_EPOCH_OFFSET_S));
    }

    #[test]
    fn epoch_micros_attribute_converts_to_unix() {
        // TimeSynchronization (56) UTCTime (0) is typed epoch_us in the IDL, so
        // attr_value_to_json must pick the offset up from the gen tables.
        let bytes = build(|w| { w.u64(&TLVTag::Anonymous, 100).unwrap(); });
        let v = attr_value_to_json(56, 0, &TLVElement::new(&bytes)).unwrap();
        assert_eq!(v, json!(100u64 + MATTER_EPOCH_OFFSET_US));
    }

    #[test]
    fn epoch_read_overflow_errors() {
        // Emitting the unshifted value would look like a valid Unix timestamp
        // and the reverse trip would "succeed" on a different instant.
        assert!(apply_epoch("epoch_us", json!(u64::MAX)).is_err());
        // A nullable epoch that came back Null still passes through.
        assert_eq!(apply_epoch("epoch_s", Value::Null).unwrap(), Value::Null);
    }

    #[test]
    fn epoch_write_bounds_are_checked() {
        // Below the Matter epoch: not a Unix timestamp at all.
        assert!(write(&json!(100), hint("epoch_s")).is_err());
        assert!(write(&json!(100), hint("epoch_us")).is_err());
        // epoch_s is a u32 on the wire, so a Unix time past 2106 doesn't fit.
        assert!(write(&json!(MATTER_EPOCH_OFFSET_S + u32::MAX as u64), hint("epoch_s")).is_ok());
        assert!(write(&json!(MATTER_EPOCH_OFFSET_S + u32::MAX as u64 + 1), hint("epoch_s")).is_err());
    }

    #[test]
    fn attr_value_of_unknown_cluster_passes_through() {
        let bytes = build(|w| { w.u32(&TLVTag::Anonymous, 100).unwrap(); });
        assert_eq!(attr_value_to_json(0xFFF1_0001, 0, &TLVElement::new(&bytes)).unwrap(), json!(100));
    }

    /// Node 12's live 0/52/0 skip: SoftwareDiagnostics.threadMetrics carries a
    /// char_string<8> thread name that real firmware fills with non-UTF-8 bytes.
    /// rs-matter's TLVValue hard-fails those with TLVTypeMismatch
    /// (rs-matter-ref/rs-matter/src/tlv/read.rs:229-240); matter.js decodes
    /// lossily (JS string semantics), so Node reported the attribute fine. JSON
    /// cannot carry invalid UTF-8 either way: replace, like Node, never drop.
    #[test]
    fn invalid_utf8_string_converts_lossily_instead_of_failing() {
        // Anonymous Utf8l(1-byte len): 0xFF is not valid UTF-8, 'b' is.
        let raw = [0x0C, 0x02, 0xFF, b'b'];
        let v = tlv_to_json(&TLVElement::new(&raw)).expect("lossy, not an error");
        assert_eq!(v, json!("\u{FFFD}b"));

        // Valid UTF-8 is byte-identical to before.
        let ok = [0x0C, 0x02, b'h', b'i'];
        assert_eq!(tlv_to_json(&TLVElement::new(&ok)).unwrap(), json!("hi"));

        // Octet strings (0x10 = 1-octet length) still go out as base64, not text.
        let oct = [0x10, 0x02, 0xFF, 0x00];
        assert_eq!(tlv_to_json(&TLVElement::new(&oct)).unwrap(), json!("/wA="));
    }

    /// A struct-typed field whose element is an invalid-UTF-8 string converts
    /// lossily instead of hard-failing.
    #[test]
    fn invalid_utf8_in_struct_field_converts_lossily_via_named_path() {
        // GroupKeyManagement(63) KeySetWriteRequest.groupKeySet (field 0) is
        // typed GroupKeySetStruct. Write a valid UTF-8 string in that slot
        // (instead of a nested struct), then corrupt its first payload byte
        // to invalid UTF-8 — this drives tlv_to_json_named_at into finding
        // field 0 and calling named_field_to_json on it, unlike a bare
        // top-level element, which returns from tlv_to_json_named's earlier
        // "not a struct" guard without ever reaching named_field_to_json.
        let cluster = matter_rs_gen::cluster(63).unwrap();
        let input = cluster.find_struct("KeySetWriteRequest").unwrap();
        let mut bytes = build(|w| {
            w.start_struct(&TLVTag::Anonymous).unwrap();
            w.utf8(&TLVTag::Context(0), "ay").unwrap();
            w.end_container().unwrap();
        });
        // 'a' (0x61) appears exactly once, as the string's first payload byte.
        let a = bytes.iter().position(|&b| b == b'a').unwrap();
        bytes[a] = 0xFF;
        let v = tlv_to_json_named(&TLVElement::new(&bytes), input.fields, cluster).unwrap();
        assert_eq!(v, json!({"groupKeySet": "\u{FFFD}y"}));
    }

    /// Nested epoch fields pass through raw, matching Node's actual wire
    /// bytes: matter.js's WS wire carries Matter-epoch values at every depth
    /// (`TimeSyncCommandsTest.ts:61` — the JS layer's own `utcTime` is Unix
    /// µs; `:64`/`:173-176` pin `MATTER_EPOCH_OFFSET_US` as positive), and
    /// `Converters.ts:394-399` subtracts that offset converting matter -> WS,
    /// re-encoding back to Matter epoch before the value reaches the wire.
    /// TimeSynchronization.timeZone (56/5) is a list of TimeZoneStruct whose
    /// field 1 validAt is epoch_us: the nested value must stay raw. Top-level
    /// epoch attributes still convert to Unix (pre-existing plan-2 behaviour,
    /// README "Accepted parity gaps" #7 — open maintainer question, not
    /// changed here).
    #[test]
    fn nested_epoch_fields_pass_through_raw_top_level_still_converts() {
        // [ { 0: offset=3600, 1: validAt=0 (Matter epoch), 2: "CET" } ]
        let bytes = {
            let mut buf = [0u8; 128];
            let mut wb = WriteBuf::new(&mut buf);
            wb.start_array(&TLVTag::Anonymous).unwrap();
            wb.start_struct(&TLVTag::Anonymous).unwrap();
            wb.i32(&TLVTag::Context(0), 3600).unwrap();
            wb.u64(&TLVTag::Context(1), 0).unwrap();
            wb.utf8(&TLVTag::Context(2), "CET").unwrap();
            wb.end_container().unwrap();
            wb.end_container().unwrap();
            wb.as_slice().to_vec()
        };
        let v = attr_value_to_json(56, 5, &TLVElement::new(&bytes)).unwrap();
        assert_eq!(
            v,
            json!([{"0": 3600, "1": 0, "2": "CET"}]),
            "validAt is nested: it must pass through raw, matching Node's wire"
        );

        // Top-level epoch attributes keep working: 56/0 UTCTime is epoch_us.
        let top = {
            let mut buf = [0u8; 16];
            let mut wb = WriteBuf::new(&mut buf);
            wb.u64(&TLVTag::Anonymous, 0).unwrap();
            wb.as_slice().to_vec()
        };
        assert_eq!(
            attr_value_to_json(56, 0, &TLVElement::new(&top)).unwrap(),
            json!(946_684_800_000_000u64)
        );
    }
}
