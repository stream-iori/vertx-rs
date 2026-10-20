use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum VertxError {
    #[error("no consumer registered for address: {0}")]
    NoConsumer(String),
    #[error("message type mismatch on address: {0}")]
    TypeMismatch(String),
    #[error("request timed out after {0:?}")]
    Timeout(std::time::Duration),
    #[error("reply channel closed before response arrived")]
    ReplyClosed,
    #[error("recipient failed ({code}): {message}")]
    RecipientFailure { code: i32, message: String },
    #[error("queue full: {0}")]
    QueueFull(String),
    #[error("channel closed: {0}")]
    ChannelClosed(String),
    #[error("deployment not found: {0}")]
    DeploymentNotFound(crate::DeploymentId),
    #[error("deployment {0} has been terminated")]
    DeploymentTerminated(crate::DeploymentId),
    #[error("Vertx has been closed")]
    VertxClosed,
    #[error("operation requires the owning Context and execution thread")]
    ContextMismatch,
    #[error("waiting for this operation from its own Context would deadlock")]
    WouldDeadlock,
    #[error("shutdown did not complete within {0:?}; cleanup continues")]
    ShutdownTimeout(std::time::Duration),
    #[error("timer was cancelled")]
    TimerCancelled,
    #[error("invalid options: {0}")]
    InvalidOptions(String),
    #[error("verticle start failed: {0}")]
    StartFailed(String),
    #[error("task panicked: {0}")]
    Panic(String),
    #[error("join error: {0}")]
    Join(String),
    #[error("deployment failed: {cause}; rollback failures: {rollback:?}")]
    DeploymentFailed {
        cause: Box<VertxError>,
        rollback: Vec<VertxError>,
    },
    #[error("cleanup failures: {0:?}")]
    CleanupFailed(Vec<VertxError>),
    #[error("other: {0}")]
    Other(String),
}
impl From<tokio::task::JoinError> for VertxError {
    fn from(e: tokio::task::JoinError) -> Self {
        Self::Join(e.to_string())
    }
}
pub(crate) fn panic_error(p: Box<dyn std::any::Any + Send>) -> VertxError {
    let message = p
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_else(|| "non-string panic".into());
    VertxError::Panic(message)
}
pub type Result<T> = std::result::Result<T, VertxError>;
