//! Arg-parsing helpers shared by all command family modules.

pub mod commissioning;
pub mod credentials;
pub mod fabrics;
pub mod interaction;
pub mod misc;
pub mod nodes;

use serde_json::{Map, Value};

use crate::api::CommandError;
use matter_rs_wire::error::ServerErrorCode;

pub fn err(code: ServerErrorCode, msg: impl Into<String>) -> CommandError { CommandError::new(code, msg) }
pub fn invalid(msg: impl Into<String>) -> CommandError { err(ServerErrorCode::InvalidArguments, msg) }

pub fn require_u64(args: &Map<String, Value>, key: &str) -> Result<u64, CommandError> {
    args.get(key).and_then(Value::as_u64)
        .ok_or_else(|| invalid(format!("missing or invalid required argument: {key}")))
}
pub fn opt_u64(args: &Map<String, Value>, key: &str) -> Option<u64> { args.get(key).and_then(Value::as_u64) }
pub fn opt_bool(args: &Map<String, Value>, key: &str) -> Option<bool> { args.get(key).and_then(Value::as_bool) }
pub fn opt_str<'a>(args: &'a Map<String, Value>, key: &str) -> Option<&'a str> { args.get(key).and_then(Value::as_str) }
pub fn require_str<'a>(args: &'a Map<String, Value>, key: &str) -> Result<&'a str, CommandError> {
    opt_str(args, key).ok_or_else(|| invalid(format!("missing or invalid required argument: {key}")))
}

/// StackError -> wire error. `default_code` lets commissioning map to 1, interview to 2, etc.
pub fn stack_err(default_code: ServerErrorCode, e: crate::stack_api::StackError) -> CommandError {
    use crate::stack_api::StackErrorKind::*;
    let code = match e.kind {
        InvalidArguments => ServerErrorCode::InvalidArguments,
        NodeUnreachable => ServerErrorCode::NodeNotResolving,
        Busy | Timeout | Sdk => default_code,
    };
    err(code, e.message)
}
