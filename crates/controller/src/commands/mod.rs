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

/// Narrows a client-supplied `u64` to a smaller Matter id/scalar, *validating*
/// instead of truncating.
///
/// This is the one class of malformed request that used to produce a successful
/// operation on a **different target** rather than an error: `70000 as u16` is
/// 4464, so `"70000/6/0"` read — or with `write_attribute`/`set_acl_entry`,
/// wrote — endpoint 4464, and `fabric_index: 256 as u8` is 0. The device side of
/// this branch validates every numeric range meticulously (`tlv_json`'s
/// `write_unsigned`/`write_signed`, `vid_pid_from`); the client side must too.
///
/// `what` names the field the way the client spelled it, so the reply says which
/// argument to fix.
pub fn narrow<T: TryFrom<u64>>(value: u64, what: &str) -> Result<T, CommandError> {
    T::try_from(value).map_err(|_| invalid(format!("{what} out of range: {value}")))
}

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
