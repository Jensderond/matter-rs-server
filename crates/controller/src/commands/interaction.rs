use serde_json::{json, Map, Value};

use matter_rs_wire::error::ServerErrorCode;

use crate::api::CommandError;
use crate::commands::{err, invalid, narrow, opt_bool, opt_u64, require_str, require_u64, stack_err};
use crate::real::MatterController;
use crate::stack_api::AttributePathSpec;

/// Node splitAttributePath: decimal segments; non-numeric OR the sentinels
/// 0xffff (endpoint) / 0xffffffff (cluster, attribute) mean wildcard.
///
/// A segment that parses but does not fit its id width is an error, never a
/// truncation: `"70000/6/0"` used to read (and via `write_attribute`, write)
/// endpoint 4464.
pub fn parse_attribute_path(path: &str) -> Result<AttributePathSpec, CommandError> {
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() != 3 {
        return Err(invalid(format!("Invalid attribute path: {path}")));
    }
    let seg = |s: &str, sentinel: u64| -> Option<u64> {
        match s.parse::<u64>() {
            Ok(n) if n == sentinel => None,
            Ok(n) => Some(n),
            Err(_) => None, // '*' or anything non-numeric
        }
    };
    Ok(AttributePathSpec {
        endpoint: seg(parts[0], 0xFFFF)
            .map(|n| narrow(n, &format!("endpoint id in attribute path {path:?}"))).transpose()?,
        cluster: seg(parts[1], 0xFFFF_FFFF)
            .map(|n| narrow(n, &format!("cluster id in attribute path {path:?}"))).transpose()?,
        attribute: seg(parts[2], 0xFFFF_FFFF)
            .map(|n| narrow(n, &format!("attribute id in attribute path {path:?}"))).transpose()?,
    })
}

pub async fn read_attribute(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    let fabric_filtered = opt_bool(args, "fabric_filtered").unwrap_or(false);
    let raw = args.get("attribute_path")
        .ok_or_else(|| invalid("missing or invalid required argument: attribute_path"))?;
    let path_strings: Vec<String> = match raw {
        Value::String(s) => vec![s.clone()],
        Value::Array(a) => a.iter()
            .map(|v| v.as_str().map(String::from)
                .ok_or_else(|| invalid("attribute_path entries must be strings")))
            .collect::<Result<_, _>>()?,
        _ => return Err(invalid("attribute_path must be a string or list of strings")),
    };
    let paths = path_strings.iter().map(|p| parse_attribute_path(p)).collect::<Result<Vec<_>, _>>()?;
    let values = c.stack.read_attributes(node_id, paths, fabric_filtered).await
        .map_err(|e| stack_err(ServerErrorCode::SdkStackError, e))?;
    if values.is_empty() {
        return Err(err(ServerErrorCode::SdkStackError, "Failed to read attribute: no values returned"));
    }
    Ok(Value::Object(values.into_iter().collect()))
}

pub async fn write_attribute(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    let path = require_str(args, "attribute_path")?;
    let spec = parse_attribute_path(path)?;
    let (Some(endpoint), Some(cluster), Some(attribute)) = (spec.endpoint, spec.cluster, spec.attribute) else {
        return Err(invalid("write_attribute does not support wildcards in attribute path"));
    };
    let value = args.get("value").cloned().unwrap_or(Value::Null);
    let status = c.stack.write_attribute(node_id, endpoint, cluster, attribute, value).await
        .map_err(|e| stack_err(ServerErrorCode::SdkStackError, e))?;
    Ok(json!([{
        "Path": { "EndpointId": endpoint, "ClusterId": cluster, "AttributeId": attribute },
        "Status": status
    }]))
}

pub async fn device_command(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    let endpoint: u16 = narrow(require_u64(args, "endpoint_id")?, "endpoint_id")?;
    let cluster: u32 = narrow(require_u64(args, "cluster_id")?, "cluster_id")?;
    let command_name = require_str(args, "command_name")?.to_string();
    let payload = args.get("payload").cloned().unwrap_or_else(|| json!({}));
    // Truncating this one is survivable today — `ops::interact::normalize_timed`
    // filters `Some(0)`, so a 65536 that landed as 0 degrades to the 10s default
    // rather than sending an already-expired request — but it is still silently
    // not the budget the client asked for.
    let timed_ms: Option<u16> = opt_u64(args, "timed_request_timeout_ms")
        .map(|v| narrow(v, "timed_request_timeout_ms")).transpose()?;
    c.stack.invoke(node_id, endpoint, cluster, command_name, payload, timed_ms).await
        .map_err(|e| stack_err(ServerErrorCode::SdkStackError, e))
}

#[cfg(test)]
mod tests {
    use crate::real::test_rig::*;
    use serde_json::json;

    #[tokio::test]
    async fn read_attribute_single_and_wildcard_paths() {
        let r = rig_with_nodes(vec![node_record(5)]);
        *r.stack.read_response.lock().unwrap() =
            Some(Ok(vec![("1/6/0".into(), json!(true)), ("2/6/0".into(), json!(false))]));
        let v = call(&r, "read_attribute", json!({"node_id": 5, "attribute_path": "*/6/0"})).await.unwrap();
        assert_eq!(v, json!({"1/6/0": true, "2/6/0": false}));
        assert!(r.stack.calls().iter().any(|c| c == "read node=5 paths=1 ff=false"));
    }

    #[tokio::test]
    async fn read_attribute_accepts_path_list_and_sentinels() {
        let r = rig_with_nodes(vec![node_record(5)]);
        *r.stack.read_response.lock().unwrap() = Some(Ok(vec![("1/6/0".into(), json!(true))]));
        let v = call(&r, "read_attribute",
            json!({"node_id": 5, "attribute_path": ["1/6/0", "65535/4294967295/4294967295"]})).await.unwrap();
        assert_eq!(v["1/6/0"], true);
        assert!(r.stack.calls().iter().any(|c| c == "read node=5 paths=2 ff=false"));
    }

    #[tokio::test]
    async fn read_attribute_empty_result_is_sdk_error() {
        let r = rig_with_nodes(vec![node_record(5)]);
        *r.stack.read_response.lock().unwrap() = Some(Ok(vec![]));
        let e = call(&r, "read_attribute", json!({"node_id": 5, "attribute_path": "1/6/0"})).await.unwrap_err();
        assert_eq!(e.code.code(), 7);
        assert_eq!(e.details, "Failed to read attribute: no values returned");
    }

    #[tokio::test]
    async fn write_attribute_rejects_wildcards_and_returns_pascal_case() {
        let r = rig_with_nodes(vec![node_record(5)]);
        let e = call(&r, "write_attribute",
            json!({"node_id": 5, "attribute_path": "*/6/0", "value": true})).await.unwrap_err();
        assert_eq!(e.code.code(), 8);
        assert_eq!(e.details, "write_attribute does not support wildcards in attribute path");

        let v = call(&r, "write_attribute",
            json!({"node_id": 5, "attribute_path": "1/8/16385", "value": 100})).await.unwrap();
        assert_eq!(v, json!([{"Path": {"EndpointId": 1, "ClusterId": 8, "AttributeId": 16385}, "Status": 0}]));
    }

    #[tokio::test]
    async fn device_command_passes_through() {
        let r = rig_with_nodes(vec![node_record(5)]);
        *r.stack.invoke_response.lock().unwrap() = Some(Ok(serde_json::Value::Null));
        let v = call(&r, "device_command", json!({
            "node_id": 5, "endpoint_id": 1, "cluster_id": 6,
            "command_name": "toggle", "payload": {}})).await.unwrap();
        assert_eq!(v, serde_json::Value::Null);
        assert!(r.stack.calls().iter().any(|c| c == "invoke node=5 1/6 toggle timed=None"));
    }

    /// Important-2 regression: a segment that does not fit its id width is an
    /// error, not a truncation. `70000 as u16` is 4464, so this used to be a
    /// *successful* read of a different endpoint — and through `write_attribute`,
    /// a successful write to one.
    #[tokio::test]
    async fn out_of_range_attribute_path_segments_are_rejected_not_truncated() {
        let r = rig_with_nodes(vec![node_record(5)]);
        *r.stack.read_response.lock().unwrap() = Some(Ok(vec![("1/6/0".into(), json!(true))]));
        let e = call(&r, "read_attribute", json!({"node_id": 5, "attribute_path": "70000/6/0"}))
            .await.unwrap_err();
        assert_eq!(e.code.code(), 8);
        assert_eq!(e.details, "endpoint id in attribute path \"70000/6/0\" out of range: 70000");
        // Nothing was sent to the stack — the point is that no OTHER endpoint got
        // read. (Not `calls().is_empty()`: the rig's supervisor kick-off task may
        // have logged a `start_supervisor` by now.)
        assert!(!r.stack.calls().iter().any(|c| c.starts_with("read ")));

        // 0xffff/0xffffffff stay the wildcard sentinels, one below them still parses.
        let e = call(&r, "write_attribute",
            json!({"node_id": 5, "attribute_path": "1/4294967296/0", "value": 1})).await.unwrap_err();
        assert_eq!(e.code.code(), 8);
        assert!(e.details.starts_with("cluster id in attribute path"), "{}", e.details);
        let v = call(&r, "write_attribute",
            json!({"node_id": 5, "attribute_path": "65534/4294967294/0", "value": 1})).await.unwrap();
        assert_eq!(v[0]["Path"]["EndpointId"], 65534);

        // device_command's own two ids, and the timed-request budget.
        let e = call(&r, "device_command", json!({"node_id": 5, "endpoint_id": 65536,
            "cluster_id": 6, "command_name": "toggle"})).await.unwrap_err();
        assert_eq!(e.details, "endpoint_id out of range: 65536");
        let e = call(&r, "device_command", json!({"node_id": 5, "endpoint_id": 1,
            "cluster_id": 6, "command_name": "toggle", "timed_request_timeout_ms": 65536}))
            .await.unwrap_err();
        assert_eq!(e.details, "timed_request_timeout_ms out of range: 65536");
    }

    #[tokio::test]
    async fn device_command_unknown_node() {
        let r = rig();
        let e = call(&r, "device_command", json!({
            "node_id": 9, "endpoint_id": 1, "cluster_id": 6, "command_name": "toggle", "payload": {}})).await.unwrap_err();
        assert_eq!(e.code.code(), 5);
        assert_eq!(e.details, "Node 9 does not exist");
    }
}
