use thiserror::Error;

/// Runtime, lifecycle and local messaging errors with stable categories.
#[derive(Debug, Clone, Error)]
pub enum VertxError {
    /// No consumer was registered when the address was resolved.
    #[error("no consumer registered for address: {0}")]
    NoConsumer(String),
    /// The address protocol, message body, or reply type differs from the expected one.
    #[error("message type mismatch on address: {0}")]
    TypeMismatch(String),
    /// The total request budget elapsed during admission or reply waiting.
    #[error("request timed out after {0:?}")]
    Timeout(std::time::Duration),
    /// The other end of a one-shot reply channel has gone away.
    #[error("reply channel closed before response arrived")]
    ReplyClosed,
    /// A consumer explicitly rejected the request with an application error.
    #[error("recipient failed ({code}): {message}")]
    RecipientFailure {
        /// Application-defined failure code.
        code: i32,
        /// Failure explanation supplied by the recipient.
        message: String,
    },
    /// The selected consumer had no free slot for immediate admission.
    #[error("queue full: {0}")]
    QueueFull(String),
    /// The selected consumer queue or local send adapter has closed.
    #[error("channel closed: {0}")]
    ChannelClosed(String),
    /// No registered deployment matches the specified ID.
    #[error("deployment not found: {0}")]
    DeploymentNotFound(crate::DeploymentId),
    /// The owning deployment is stopping or has already finished.
    #[error("deployment {0} has been terminated")]
    DeploymentTerminated(crate::DeploymentId),
    /// The instance no longer accepts ordinary work.
    #[error("Vertx has been closed")]
    VertxClosed,
    /// The operation requires a different logical Context or physical thread.
    #[error("operation requires the owning Context and execution thread")]
    ContextMismatch,
    /// The caller would wait for execution or cleanup that depends on itself.
    #[error("waiting for this operation from its own Context would deadlock")]
    WouldDeadlock,
    /// The wait deadline elapsed while background cleanup was still pending.
    #[error("shutdown did not complete within {0:?}; cleanup continues")]
    ShutdownTimeout(std::time::Duration),
    /// The timer or its owning scope was cancelled before the tick.
    #[error("timer was cancelled")]
    TimerCancelled,
    /// An argument violated a documented configuration bound.
    #[error("invalid options: {0}")]
    InvalidOptions(String),
    /// A socket operation failed on a managed TCP address.
    #[error("TCP {operation} on {addr} failed: {message}")]
    TcpIo {
        /// Socket operation, such as `bind` or `accept`.
        operation: &'static str,
        /// Local address associated with the operation.
        addr: std::net::SocketAddr,
        /// OS I/O error category for programmatic handling.
        kind: std::io::ErrorKind,
        /// Underlying error text for diagnosis.
        message: String,
    },
    /// Startup ended without a readiness result from the lifecycle driver.
    #[error("verticle start failed: {0}")]
    StartFailed(String),
    /// A user callback or task unwound at a framework boundary.
    #[error("task panicked: {0}")]
    Panic(String),
    /// A managed task was aborted before producing a result.
    #[error("task was cancelled")]
    TaskCancelled,
    /// An executor result could not be joined or received.
    #[error("join error: {0}")]
    Join(String),
    /// A batch deployment failed and rollback ran for its earlier instances.
    #[error("deployment failed: {cause}; rollback failures: {rollback:?}")]
    DeploymentFailed {
        /// Error that stopped the batch.
        #[source]
        cause: Box<VertxError>,
        /// Errors encountered while rolling back earlier instances.
        rollback: Vec<VertxError>,
    },
    /// An operation failed and its cleanup produced additional errors.
    #[error("lifecycle failed: {cause}; cleanup failures: {cleanup:?}")]
    LifecycleFailed {
        /// First failure observed by the operation.
        #[source]
        cause: Box<VertxError>,
        /// Subsequent failures in cleanup order.
        cleanup: Vec<VertxError>,
    },
    /// Collected failures from instance shutdown and executor cleanup.
    #[error("cleanup failures: {0:?}")]
    CleanupFailed(Vec<VertxError>),
    /// An application or runtime failure with a textual explanation.
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
/// Result returned by vertx-rs operations.
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
            Self::TcpIo { .. } => "tcp_io",
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
