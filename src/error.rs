use thiserror::Error;

#[derive(Debug, Error)]
pub enum VertxError {
    #[error("no consumer registered for address: {0}")]
    NoConsumer(String),

    #[error("message type mismatch on address: {0}")]
    TypeMismatch(String),

    #[error("request timed out after {0:?}")]
    Timeout(std::time::Duration),

    #[error("reply channel closed before response arrived")]
    ReplyClosed,

    #[error("deployment not found: {0:?}")]
    DeploymentNotFound(crate::DeploymentId),

    #[error("deployment {0} has been terminated")]
    DeploymentTerminated(crate::DeploymentId),

    #[error("timer was cancelled")]
    TimerCancelled,

    #[error("verticle start failed: {0}")]
    StartFailed(String),

    #[error("join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("other: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, VertxError>;
