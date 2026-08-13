use serde_json::{json, Value};
use tokio::sync::broadcast;

use matter_rs_wire::envelope::{CommandMessage, EventMessage};
use matter_rs_wire::error::ServerErrorCode;
use matter_rs_wire::server_info::ServerInfoMessage;

use crate::api::{CommandError, Controller};

/// Protocol-skeleton stand-in: real shapes for session commands, honest
/// errors for the rest. Replaced by the rs-matter-backed controller in plan 2.
pub struct StubController {
    info: ServerInfoMessage,
    events: broadcast::Sender<EventMessage>,
}

impl StubController {
    pub fn new(info: ServerInfoMessage) -> Self {
        let (events, _) = broadcast::channel(256);
        Self { info, events }
    }

    pub fn event_sender(&self) -> broadcast::Sender<EventMessage> {
        self.events.clone()
    }
}

#[async_trait::async_trait]
impl Controller for StubController {
    fn server_info(&self) -> ServerInfoMessage {
        self.info.clone()
    }

    fn node_count(&self) -> usize {
        0
    }

    async fn handle_command(&self, cmd: &CommandMessage) -> Result<Value, CommandError> {
        match cmd.command.as_str() {
            "server_info" => Ok(serde_json::to_value(self.server_info()).unwrap()),
            "start_listening" | "get_nodes" => Ok(json!([])),
            "diagnostics" => Ok(json!({
                "info": serde_json::to_value(self.server_info()).unwrap(),
                "nodes": [],
                "events": [],
            })),
            "get_node" | "interview_node" | "remove_node" | "ping_node"
            | "get_node_ip_addresses" | "read_attribute" | "write_attribute"
            | "device_command" => Err(CommandError::new(
                ServerErrorCode::NodeNotExists,
                "stub controller has no nodes",
            )),
            other => Err(CommandError::new(
                ServerErrorCode::InvalidCommand,
                format!("unknown command: {other}"),
            )),
        }
    }

    fn subscribe_events(&self) -> broadcast::Receiver<EventMessage> {
        self.events.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::Controller;
    use matter_rs_wire::envelope::CommandMessage;
    use matter_rs_wire::server_info::{ServerInfoMessage, MIN_SUPPORTED_SCHEMA_VERSION, SCHEMA_VERSION};
    use serde_json::json;

    fn test_info() -> ServerInfoMessage {
        ServerInfoMessage {
            fabric_id: 1,
            compressed_fabric_id: 0,
            fabric_index: None,
            schema_version: SCHEMA_VERSION,
            min_supported_schema_version: MIN_SUPPORTED_SCHEMA_VERSION,
            sdk_version: "test".into(),
            wifi_credentials_set: false,
            wifi_ssid: None,
            thread_credentials_set: false,
            bluetooth_enabled: false,
            ble_proxy_enabled: None,
            controller_node_id: None,
        }
    }

    fn cmd(name: &str) -> CommandMessage {
        serde_json::from_value(json!({"message_id": "1", "command": name})).unwrap()
    }

    #[tokio::test]
    async fn known_read_commands_return_empty_shapes() {
        let c = StubController::new(test_info());
        assert_eq!(c.handle_command(&cmd("get_nodes")).await.unwrap(), json!([]));
        assert_eq!(c.handle_command(&cmd("start_listening")).await.unwrap(), json!([]));
        let si = c.handle_command(&cmd("server_info")).await.unwrap();
        assert_eq!(si["schema_version"], 13);
        let diag = c.handle_command(&cmd("diagnostics")).await.unwrap();
        assert_eq!(diag["nodes"], json!([]));
        assert_eq!(diag["events"], json!([]));
    }

    #[tokio::test]
    async fn get_node_returns_node_not_exists() {
        let c = StubController::new(test_info());
        let err = c.handle_command(&cmd("get_node")).await.unwrap_err();
        assert_eq!(err.code.code(), 5);
    }

    #[tokio::test]
    async fn unknown_command_returns_invalid_command() {
        let c = StubController::new(test_info());
        let err = c.handle_command(&cmd("frobnicate")).await.unwrap_err();
        assert_eq!(err.code.code(), 9);
    }

    #[tokio::test]
    async fn events_flow_through_broadcast() {
        let c = StubController::new(test_info());
        let mut rx = c.subscribe_events();
        c.event_sender()
            .send(matter_rs_wire::envelope::EventMessage { event: "node_added".into(), data: json!({}) })
            .unwrap();
        assert_eq!(rx.recv().await.unwrap().event, "node_added");
    }
}
