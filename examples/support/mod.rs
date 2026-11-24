//! Utilities shared by the examples.
use std::future::Future;
use std::time::Duration;
use vertx_rs::{Result, Vertx, VertxError};

pub fn init_tracing() {
    let filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into());
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if std::env::var("LOG_FORMAT").as_deref() == Ok("json") {
        builder.json().init();
    } else {
        builder.init();
    }
}

pub async fn bounded(future: impl Future<Output = Result<()>>) -> Result<()> {
    let limit = Duration::from_secs(5);
    tokio::time::timeout(limit, future)
        .await
        .unwrap_or(Err(VertxError::Timeout(limit)))
}

pub async fn finish(vx: &Vertx, outcome: Result<()>) -> Result<()> {
    let result = match (outcome, vx.close().await) {
        (Ok(()), close) => close,
        (Err(cause), Ok(())) => Err(cause),
        (Err(cause), Err(cleanup)) => Err(VertxError::LifecycleFailed {
            cause: Box::new(cause),
            cleanup: vec![cleanup],
        }),
    };
    if result.is_ok() {
        let snapshot = vx.diagnostics();
        assert_eq!(snapshot.active_deployments, 0);
        assert_eq!(snapshot.active_tasks, 0);
        assert_eq!(snapshot.ready_tasks, 0);
        assert_eq!(snapshot.active_consumers, 0);
        assert_eq!(
            snapshot.queued_messages + snapshot.reserved_slots + snapshot.waiting_senders,
            0
        );
        assert_eq!(snapshot.blocking_running + snapshot.blocking_waiting, 0);
    }
    result
}
