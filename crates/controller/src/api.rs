use matter_rs_wire::envelope::{CommandMessage, EventMessage};
use matter_rs_wire::error::ServerErrorCode;
use matter_rs_wire::server_info::ServerInfoMessage;

#[derive(Debug)]
pub struct CommandError {
    pub code: ServerErrorCode,
    pub details: String,
}

impl CommandError {
    pub fn new(code: ServerErrorCode, details: impl Into<String>) -> Self {
        Self { code, details: details.into() }
    }
}

#[async_trait::async_trait]
pub trait Controller: Send + Sync + 'static {
    fn server_info(&self) -> ServerInfoMessage;
    fn node_count(&self) -> usize;
    async fn handle_command(&self, cmd: &CommandMessage) -> Result<serde_json::Value, CommandError>;
    fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<EventMessage>;
}
