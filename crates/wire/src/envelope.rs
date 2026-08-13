use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ServerErrorCode;

/// Inbound request: {"message_id", "command", "args"?}.
#[derive(Debug, Clone, Deserialize)]
pub struct CommandMessage {
    pub message_id: String,
    pub command: String,
    #[serde(default)]
    pub args: serde_json::Map<String, Value>,
}

/// Outbound success: {"message_id", "result"}.
#[derive(Debug, Clone, Serialize)]
pub struct SuccessResult {
    pub message_id: String,
    pub result: Value,
}

/// Outbound error: {"message_id", "error_code", "details"}.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorResult {
    pub message_id: String,
    pub error_code: u16,
    pub details: String,
}

impl ErrorResult {
    pub fn new(message_id: String, code: ServerErrorCode, details: String) -> Self {
        Self { message_id, error_code: code.code(), details }
    }
}

/// Outbound event: {"event", "data"} — only after start_listening.
#[derive(Debug, Clone, Serialize)]
pub struct EventMessage {
    pub event: String,
    pub data: Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_command_with_and_without_args() {
        let m: CommandMessage =
            serde_json::from_str(r#"{"message_id":"1","command":"get_node","args":{"node_id":42}}"#)
                .unwrap();
        assert_eq!(m.message_id, "1");
        assert_eq!(m.command, "get_node");
        assert_eq!(m.args.get("node_id"), Some(&json!(42)));

        let m: CommandMessage =
            serde_json::from_str(r#"{"message_id":"2","command":"server_info"}"#).unwrap();
        assert!(m.args.is_empty());
    }

    #[test]
    fn success_result_shape() {
        let s = SuccessResult { message_id: "1".into(), result: json!([]) };
        assert_eq!(serde_json::to_string(&s).unwrap(), r#"{"message_id":"1","result":[]}"#);
    }

    #[test]
    fn error_result_shape() {
        let e = ErrorResult::new("1".into(), crate::error::ServerErrorCode::InvalidCommand, "nope".into());
        assert_eq!(
            serde_json::to_string(&e).unwrap(),
            r#"{"message_id":"1","error_code":9,"details":"nope"}"#
        );
    }

    #[test]
    fn event_shape_and_big_u64_stays_numeric() {
        // node ids can exceed 2^53; they must serialize as unquoted numbers.
        let ev = EventMessage { event: "node_added".into(), data: json!({"node_id": 18446744073709551615u64}) };
        assert_eq!(
            serde_json::to_string(&ev).unwrap(),
            r#"{"event":"node_added","data":{"node_id":18446744073709551615}}"#
        );
    }
}
