//! Session/node command family: server_info fan-out helpers, get/list nodes,
//! interview, remove, ping, and IP address lookup.

use serde_json::{json, Map, Value};

use matter_rs_wire::error::ServerErrorCode;

use crate::api::CommandError;
use crate::commands::{opt_bool, opt_u64_strict, require_u64, stack_err};
use crate::lock::lock;
use crate::real::MatterController;
use crate::storage::format_node_date;

/// Upper bound on `ping_node`'s client-supplied `attempts`.
const MAX_PING_ATTEMPTS: u64 = 10;

/// Clamped, not validated: `attempts` becomes `ping -c <attempts>` with a 1s
/// interval, and `futures_join_all` walks the addresses sequentially, so an
/// unclamped `attempts: 1000` would occupy a command handler for ~1000s *per
/// address*. Nothing legitimate asks for more than a handful, and silently
/// capping beats failing a request that is merely over-eager. A value that is
/// not a u64 at all is a different matter: that is an error, not over-eagerness.
fn ping_attempts(args: &Map<String, Value>) -> Result<u64, CommandError> {
    Ok(opt_u64_strict(args, "attempts")?.unwrap_or(1).clamp(1, MAX_PING_ATTEMPTS))
}

pub async fn get_nodes(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let only_available = opt_bool(args, "only_available").unwrap_or(false);
    Ok(serde_json::to_value(c.registry.all_node_data(only_available)).unwrap())
}

pub async fn get_node(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.registry.node_data(node_id)
        .map(|n| serde_json::to_value(n).unwrap())
        .ok_or_else(|| CommandError::new(ServerErrorCode::NodeNotExists, format!("Node {node_id} does not exist")))
}

pub async fn diagnostics(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let nodes = get_nodes(c, args).await?;
    let events: Vec<Value> = lock(&c.history).iter().cloned().collect();
    Ok(json!({ "info": c.build_server_info(), "nodes": nodes, "events": events }))
}

pub async fn interview_node(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    let attributes = c.stack.interview(node_id).await
        .map_err(|e| stack_err(ServerErrorCode::NodeInterviewFailed, e))?;
    c.registry.with_entry(node_id, |e| {
        e.record.attributes = attributes.into_iter().collect();
        e.record.last_interview = format_node_date(std::time::SystemTime::now());
    });
    if let Some(rec) = c.registry.snapshot_record(node_id) {
        if let Err(e) = c.storage.save_node(&rec) { tracing::error!("persist node {node_id}: {e}"); }
    }
    if let Some(nd) = c.registry.node_data(node_id) {
        let _ = c.events.send(matter_rs_wire::envelope::EventMessage {
            event: "node_updated".into(), data: serde_json::to_value(nd).unwrap() });
    }
    Ok(Value::Null)
}

pub async fn remove_node(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    let device_fab_idx = c.registry.snapshot_record(node_id).map(|r| r.device_fabric_index).unwrap_or(0);
    c.stack.stop_supervisor(node_id).await;
    if let Err(e) = c.stack.remove_device_fabric(node_id, device_fab_idx).await {
        tracing::warn!("RemoveFabric on node {node_id} failed ({}); removing locally anyway", e.message);
    }
    c.registry.remove(node_id);
    if let Err(e) = c.storage.delete_node(node_id) { tracing::error!("delete node file {node_id}: {e}"); }
    let _ = c.events.send(matter_rs_wire::envelope::EventMessage {
        event: "node_removed".into(), data: json!(node_id) });
    Ok(Value::Null)
}

/// Live (stack) addresses first, then cached record addresses; dedup preserving order.
async fn merged_addresses(c: &MatterController, node_id: u64) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    if let Ok(live) = c.stack.node_addresses(node_id).await {
        out.extend(live);
    }
    if let Some(rec) = c.registry.snapshot_record(node_id) {
        out.extend(rec.addresses);
    }
    let mut seen = std::collections::HashSet::new();
    out.retain(|a| seen.insert(a.clone()));
    out
}

pub async fn ping_node(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    let attempts = ping_attempts(args)?;
    let addrs = merged_addresses(c, node_id).await;
    let mut results = Map::new();
    let futures: Vec<_> = addrs.iter().map(|a| ping_one(a.clone(), attempts)).collect();
    for (addr, ok) in futures_join_all(futures).await {
        results.insert(addr, Value::Bool(ok));
    }
    Ok(Value::Object(results))
}

// Small local join_all to avoid a futures dependency.
async fn futures_join_all<T>(futs: Vec<impl std::future::Future<Output = T>>) -> Vec<T> {
    let mut out = Vec::with_capacity(futs.len());
    for f in futs { out.push(f.await); } // sequential is fine at homelab scale
    out
}

/// System ping (iputils on the Debian target; ping6 fallback for macOS dev).
async fn ping_one(addr: String, attempts: u64) -> (String, bool) {
    let bare = addr.split('%').next().unwrap_or(&addr).to_string();
    let is_v6 = bare.contains(':');
    let (bin, timeout_flag) = if cfg!(target_os = "macos") {
        (if is_v6 { "ping6" } else { "ping" }, "-t")
    } else {
        ("ping", "-W")
    };
    let mut cmd = tokio::process::Command::new(bin);
    if !cfg!(target_os = "macos") && is_v6 { cmd.arg("-6"); }
    cmd.arg("-c").arg(attempts.to_string()).arg(timeout_flag).arg("10").arg(&bare);
    cmd.stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null());
    let ok = matches!(cmd.status().await.map(|s| s.success()), Ok(true));
    (addr, ok)
}

pub async fn get_node_ip_addresses(c: &MatterController, args: &Map<String, Value>) -> Result<Value, CommandError> {
    let node_id = require_u64(args, "node_id")?;
    c.ensure_node(node_id)?;
    let scoped = opt_bool(args, "scoped").unwrap_or(false);
    let addrs: Vec<String> = merged_addresses(c, node_id).await.into_iter()
        .map(|a| if scoped { a } else { a.split('%').next().unwrap_or(&a).to_string() })
        .collect();
    let mut seen = std::collections::HashSet::new();
    let addrs: Vec<String> = addrs.into_iter().filter(|a| seen.insert(a.clone())).collect();
    Ok(serde_json::to_value(addrs).unwrap())
}

#[cfg(test)]
mod tests {
    use super::{ping_attempts, MAX_PING_ATTEMPTS};
    use serde_json::json;

    /// The clamp itself, since exercising it through `ping_node` would mean
    /// actually shelling out to `ping` for ten seconds.
    #[test]
    fn ping_attempts_is_clamped_to_a_sane_range() {
        let args = |v: serde_json::Value| v.as_object().unwrap().clone();
        assert_eq!(ping_attempts(&args(json!({}))).unwrap(), 1);
        assert_eq!(ping_attempts(&args(json!({"attempts": 0}))).unwrap(), 1);
        assert_eq!(ping_attempts(&args(json!({"attempts": 3}))).unwrap(), 3);
        assert_eq!(ping_attempts(&args(json!({"attempts": 1000}))).unwrap(), MAX_PING_ATTEMPTS);
    }

    /// A present `attempts` that is not a u64 — negative, fractional, a string —
    /// used to silently become the default via `as_u64 -> None`. It is the
    /// client's error and must be reported as one; only absent (or JSON null)
    /// means "use the default".
    #[test]
    fn ping_attempts_rejects_a_present_but_invalid_value() {
        let args = |v: serde_json::Value| v.as_object().unwrap().clone();
        for bad in [json!(-1), json!(2.5), json!("many")] {
            assert!(
                ping_attempts(&args(json!({"attempts": bad}))).is_err(),
                "attempts {bad} must be rejected"
            );
        }
        assert_eq!(ping_attempts(&args(json!({"attempts": null}))).unwrap(), 1);
    }
}
