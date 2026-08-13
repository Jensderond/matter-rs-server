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
    /// Subscribes to the controller's event broadcast stream.
    ///
    /// Implementations must own their broadcast `Sender` for the entire
    /// lifetime of the `Controller`. If the sender is dropped or replaced
    /// while connections still hold receivers from this method, those
    /// receivers see `Err(Closed)` on every subsequent `recv()`, and the
    /// server's event-handling arm degrades (silently stops delivering
    /// events) for the life of the connection.
    fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<EventMessage>;
}
