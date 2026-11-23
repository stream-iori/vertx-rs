use super::envelope::ErasedEnvelope;
use crate::diagnostics::QueueStats;
use crate::{Result, VertxError};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

/// Shared queue status. A consumer can hold this without keeping a sender alive.
pub(super) struct QueueState {
    cancel: CancellationToken,
    active: AtomicBool,
    stats: Option<Arc<QueueStats>>,
}

impl QueueState {
    pub(super) fn cancellation(&self) -> Pin<Box<WaitForCancellationFutureOwned>> {
        Box::pin(self.cancel.clone().cancelled_owned())
    }

    pub(super) fn dequeued(&self) {
        if let Some(stats) = &self.stats {
            stats.queued.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

#[derive(Clone)]
pub(super) struct QueueSender {
    tx: mpsc::Sender<ErasedEnvelope>,
    pub(super) state: Arc<QueueState>,
    address: String,
    closed: CancellationToken,
}

impl QueueSender {
    pub(super) fn channel(
        address: &str,
        capacity: usize,
        closed: CancellationToken,
        cancel: CancellationToken,
        stats: Option<Arc<QueueStats>>,
    ) -> (Self, mpsc::Receiver<ErasedEnvelope>) {
        let (tx, rx) = mpsc::channel(capacity);
        let sender = Self {
            tx,
            state: Arc::new(QueueState {
                cancel,
                active: AtomicBool::new(true),
                stats,
            }),
            address: address.into(),
            closed,
        };
        (sender, rx)
    }

    pub(super) fn mark_inactive(&self) {
        self.state.active.store(false, Ordering::Release);
    }

    pub(super) fn cancel(self) {
        self.state.cancel.cancel();
    }

    pub(super) async fn send(self, envelope: ErasedEnvelope) -> Result<()> {
        let mut reservation = self.reserve();
        poll_fn(|cx| reservation.poll_ready(cx)).await?;
        reservation.send(envelope)
    }

    pub(super) fn reserve(self) -> Reservation {
        let acquire = Box::pin(self.tx.clone().reserve_owned());
        let mut reservation = Reservation::new(self);
        reservation.acquire = Some(acquire);
        reservation
    }

    pub(super) fn try_reserve(self) -> Result<Reservation> {
        self.check_open()?;
        let permit = self
            .tx
            .clone()
            .try_reserve_owned()
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    if let Some(stats) = &self.state.stats {
                        stats.full();
                    }
                    VertxError::QueueFull(self.address.clone())
                }
                mpsc::error::TrySendError::Closed(_) => self.closed_error(),
            })?;
        let mut reservation = Reservation::new(self);
        reservation.set_permit(permit);
        Ok(reservation)
    }

    fn check_open(&self) -> Result<()> {
        if self.closed.is_cancelled() {
            Err(VertxError::VertxClosed)
        } else if !self.state.active.load(Ordering::Acquire)
            || self.state.cancel.is_cancelled()
            || self.tx.is_closed()
        {
            Err(self.closed_error())
        } else {
            Ok(())
        }
    }

    fn closed_error(&self) -> VertxError {
        VertxError::ChannelClosed(self.address.clone())
    }
}

type Permit = mpsc::OwnedPermit<ErasedEnvelope>;
type AcquireResult = std::result::Result<Permit, mpsc::error::SendError<()>>;
type Acquire = Pin<Box<dyn Future<Output = AcquireResult> + Send>>;

/// One queue position and, once ready, one slot. Drop releases both accounting
/// and Tokio's reservation even if a waiting sender is aborted before wakeup.
pub(super) struct Reservation {
    sender: QueueSender,
    acquire: Option<Acquire>,
    cancel: Pin<Box<WaitForCancellationFutureOwned>>,
    permit: Option<Permit>,
    waiting: bool,
}

impl Reservation {
    fn new(sender: QueueSender) -> Self {
        let cancel = sender.state.cancellation();
        Self {
            sender,
            acquire: None,
            cancel,
            permit: None,
            waiting: false,
        }
    }

    pub(super) fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if let Err(error) = self.sender.check_open() {
            return Poll::Ready(Err(error));
        }
        if self.cancel.as_mut().poll(cx).is_ready() {
            return Poll::Ready(self.sender.check_open());
        }
        if self.permit.is_some() {
            return Poll::Ready(Ok(()));
        }
        let acquire = self
            .acquire
            .as_mut()
            .expect("unreserved sender owns an acquire future");
        match acquire.as_mut().poll(cx) {
            Poll::Pending => {
                self.begin_wait();
                Poll::Pending
            }
            Poll::Ready(result) => Poll::Ready(self.finish_acquire(result)),
        }
    }

    fn finish_acquire(&mut self, result: AcquireResult) -> Result<()> {
        self.end_wait();
        self.acquire = None;
        let permit = result.map_err(|_| self.sender.closed_error())?;
        self.set_permit(permit);
        self.sender.check_open()
    }

    fn set_permit(&mut self, permit: Permit) {
        self.permit = Some(permit);
        if let Some(stats) = &self.sender.state.stats {
            stats.reserved.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn begin_wait(&mut self) {
        if self.waiting {
            return;
        }
        self.waiting = true;
        if let Some(stats) = &self.sender.state.stats {
            stats.waiting.fetch_add(1, Ordering::Relaxed);
            // Tokio can also yield for its cooperative budget.
            if self.sender.tx.capacity() == 0 {
                stats.full();
            }
        }
    }

    fn end_wait(&mut self) {
        if self.waiting {
            self.waiting = false;
            if let Some(stats) = &self.sender.state.stats {
                stats.waiting.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    pub(super) fn send(mut self, mut envelope: ErasedEnvelope) -> Result<()> {
        // Tracing callbacks can reenter the bus before the final liveness check.
        let trace = envelope.trace.mark_enqueued();
        self.sender.check_open()?;
        let permit = self
            .permit
            .take()
            .ok_or_else(|| VertxError::Other("sink is not ready".into()))?;
        if let Some(stats) = &self.sender.state.stats {
            stats.reserved.fetch_sub(1, Ordering::Relaxed);
            // Update counts before waking a receiver which may immediately dequeue.
            stats.queued.fetch_add(1, Ordering::Relaxed);
        }
        permit.send(envelope);
        if let Some(trace) = trace {
            trace.log_enqueued();
        }
        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.end_wait();
        if self.permit.is_some()
            && let Some(stats) = &self.sender.state.stats
        {
            stats.reserved.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
