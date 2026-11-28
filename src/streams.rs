//! Pull-based transfers executed by the caller's Future. No tasks or threads
//! are created here; use Context::spawn_local/spawn to manage the transfer.
use crate::{Result, VertxError};
use futures::{Sink, Stream};
use std::future::poll_fn;
use std::pin::pin;

/// Normal EOF always flushes. By default both success and failure also close
/// the sink. Drop/abort cannot await asynchronous cleanup.
#[derive(Debug, Clone, Copy)]
pub struct PipeOptions {
    /// Close after a successful EOF flush (default true).
    pub end_on_success: bool,
    /// Attempt close after a read/readiness/write/flush error (default true).
    pub end_on_failure: bool,
}
impl Default for PipeOptions {
    fn default() -> Self {
        Self {
            end_on_success: true,
            end_on_failure: true,
        }
    }
}
/// Transfer items in order, with at most one item awaiting admission. Does not
/// require Send or Unpin and does not change the current Context.
///
/// ```
/// # use vertx_rs::{pipe_to, Result, Vertx};
/// # async fn example(vx: &Vertx) -> Result<()> {
/// let source = futures::stream::iter([Ok(1u32), Ok(2)]);
/// let destination = vx.event_bus().sink::<u32>("numbers")?;
/// // The address must have a live u32 consumer before this future is polled.
/// pipe_to(source, destination).await?;
/// # Ok(()) }
/// ```
/// Completing a transfer to an EventBus sink confirms admission only. Headers
/// and reply capabilities are not implicitly forwarded from Envelope items.
pub async fn pipe_to<T, S, K>(source: S, sink: K) -> Result<()>
where
    S: Stream<Item = Result<T>>,
    K: Sink<T, Error = VertxError>,
{
    pipe_to_with(source, sink, PipeOptions::default()).await
}
/// Transfer with explicit ending policy. Even with end_on_success=false, EOF
/// waits for flush. If transfer and close fail, preserve both errors.
/// Cancellation drops an unsent item and reservations; previously admitted
/// messages/writes are not retracted. Rust panics follow the caller's executor
/// policy and Drop, rather than asynchronous failure cleanup.
pub async fn pipe_to_with<T, S, K>(source: S, sink: K, options: PipeOptions) -> Result<()>
where
    S: Stream<Item = Result<T>>,
    K: Sink<T, Error = VertxError>,
{
    let mut source = pin!(source);
    let mut sink = pin!(sink);
    let mut outcome = async {
        let mut batch = 0;
        while let Some(item) = poll_fn(|cx| source.as_mut().poll_next(cx)).await {
            let item = item?;
            poll_fn(|cx| sink.as_mut().poll_ready(cx)).await?;
            sink.as_mut().start_send(item)?;
            batch += 1;
            if batch == 64 {
                batch = 0;
                // Even always-ready custom streams/sinks must yield to other
                // Context tasks. This does not require a particular executor.
                let mut yielded = false;
                poll_fn(|cx| {
                    if yielded {
                        std::task::Poll::Ready(())
                    } else {
                        yielded = true;
                        cx.waker().wake_by_ref();
                        std::task::Poll::Pending
                    }
                })
                .await;
            }
        }
        tracing::trace!(stage = "flush", "pipe stage");
        poll_fn(|cx| sink.as_mut().poll_flush(cx)).await
    }
    .await;
    let close = if outcome.is_ok() {
        options.end_on_success
    } else {
        options.end_on_failure
    };
    if close {
        tracing::trace!(stage = "close", "pipe stage");
        if let Err(error) = poll_fn(|cx| sink.as_mut().poll_close(cx)).await {
            crate::error::retain_failure(&mut outcome, error);
        }
    }
    outcome
}
