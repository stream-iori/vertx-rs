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
    #[error("task was cancelled")]
    TaskCancelled,
    #[error("join error: {0}")]
    Join(String),
    #[error("deployment failed: {cause}; rollback failures: {rollback:?}")]
    DeploymentFailed {
        #[source]
        cause: Box<VertxError>,
        rollback: Vec<VertxError>,
    },
    #[error("lifecycle failed: {cause}; cleanup failures: {cleanup:?}")]
    LifecycleFailed {
        #[source]
        cause: Box<VertxError>,
        cleanup: Vec<VertxError>,
    },
    #[error("cleanup failures: {0:?}")]
    CleanupFailed(Vec<VertxError>),
    #[error("other: {0}")]
    Other(String),
}
impl From<tokio::task::JoinError> for VertxError {
    fn from(e: tokio::task::JoinError) -> Self {
        if e.is_cancelled() {
            Self::TaskCancelled
        } else if e.is_panic() {
            panic_error(e.into_panic())
        } else {
            Self::Join(e.to_string())
        }
    }
}
pub(crate) fn panic_error(p: Box<dyn std::any::Any + Send>) -> VertxError {
    panic_error_ref(p.as_ref())
}
pub(crate) fn panic_error_ref(p: &(dyn std::any::Any + Send)) -> VertxError {
    let message = p
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_else(|| "non-string panic".into());
    VertxError::Panic(message)
}
pub type Result<T> = std::result::Result<T, VertxError>;

/// Preserve the first failure and append subsequent lifecycle cleanup failures.
pub(crate) fn retain_failure(outcome: &mut Result<()>, error: VertxError) {
    match outcome {
        Ok(()) => *outcome = Err(error),
        Err(VertxError::LifecycleFailed { cleanup, .. }) => cleanup.push(error),
        Err(cause) => {
            *outcome = Err(VertxError::LifecycleFailed {
                cause: Box::new(cause.clone()),
                cleanup: vec![error],
            });
        }
    }
}

impl VertxError {
    /// Stable machine-readable classification; does not include user payloads.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::NoConsumer(_) => "no_consumer",
            Self::TypeMismatch(_) => "type_mismatch",
            Self::Timeout(_) => "timeout",
            Self::ReplyClosed => "reply_closed",
            Self::RecipientFailure { .. } => "recipient_failure",
            Self::QueueFull(_) => "queue_full",
            Self::ChannelClosed(_) => "channel_closed",
            Self::DeploymentNotFound(_) => "deployment_not_found",
            Self::DeploymentTerminated(_) => "deployment_terminated",
            Self::VertxClosed => "vertx_closed",
            Self::ContextMismatch => "context_mismatch",
            Self::WouldDeadlock => "would_deadlock",
            Self::ShutdownTimeout(_) => "shutdown_timeout",
            Self::TimerCancelled => "timer_cancelled",
            Self::InvalidOptions(_) => "invalid_options",
            Self::StartFailed(_) => "start_failed",
            Self::Panic(_) => "panic",
            Self::TaskCancelled => "task_cancelled",
            Self::Join(_) => "join",
            Self::DeploymentFailed { .. } => "deployment_failed",
            Self::LifecycleFailed { .. } => "lifecycle_failed",
            Self::CleanupFailed(_) => "cleanup_failed",
            Self::Other(_) => "other",
        }
    }
}
