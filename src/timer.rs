use crate::{Result, VertxError};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::{Instant, Interval};
use tokio_util::sync::CancellationToken;

const PENDING: u8 = 0;
const COMPLETED: u8 = 1;
const CANCELLED: u8 = 2;

/// One-shot future whose deadline is fixed at construction, even if first
/// polled later. Cancellation wins when both branches are ready.
pub struct Timer {
    future: Pin<Box<dyn Future<Output = Result<()>> + Send>>,
    timer_cancel: CancellationToken,
    owner_cancel: CancellationToken,
    state: Arc<AtomicU8>,
}
impl Timer {
    pub(crate) fn new(delay: Duration, owner_cancel: CancellationToken) -> Result<Self> {
        let deadline = deadline_after(Instant::now(), delay)?;
        let timer_cancel = CancellationToken::new();
        let state = Arc::new(AtomicU8::new(PENDING));
        let (timer_cancel_for_poll, owner_cancel_for_poll, state_for_poll) =
            (timer_cancel.clone(), owner_cancel.clone(), state.clone());
        let future = Box::pin(async move {
            let result = tokio::select! { biased;
                _ = timer_cancel_for_poll.cancelled() => Err(VertxError::TimerCancelled),
                _ = owner_cancel_for_poll.cancelled() => Err(VertxError::TimerCancelled),
                _ = tokio::time::sleep_until(deadline) => Ok(()),
            };
            state_for_poll.store(
                if result.is_ok() { COMPLETED } else { CANCELLED },
                Ordering::Release,
            );
            result
        });
        Ok(Self {
            future,
            timer_cancel,
            owner_cancel,
            state,
        })
    }
    /// Cancels a pending timer without changing an already-completed timer.
    pub fn cancel(&self) {
        if self
            .state
            .compare_exchange(PENDING, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.timer_cancel.cancel();
        }
    }
    /// Includes explicit and owner cancellation while pending.
    /// A completed timer does not become cancelled on later close.
    pub fn is_cancelled(&self) -> bool {
        self.state.load(Ordering::Acquire) == CANCELLED
            || self.state.load(Ordering::Acquire) == PENDING && self.owner_cancel.is_cancelled()
    }
}
impl Future for Timer {
    type Output = Result<()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.future.as_mut().poll(cx)
    }
}
/// Pull-based periodic timer; creating or dropping it does not spawn work.
/// Call [`Self::tick`] to drive ticks; cancellation affects every later tick.
pub struct Periodic {
    interval: Option<Interval>,
    deadline: Instant,
    period: Duration,
    timer_cancel: CancellationToken,
    owner_cancel: CancellationToken,
}
impl Periodic {
    pub(crate) fn new(
        init: Duration,
        period: Duration,
        owner_cancel: CancellationToken,
    ) -> Result<Self> {
        if period.is_zero() {
            return Err(VertxError::InvalidOptions("period must be positive".into()));
        }
        let deadline = deadline_after(Instant::now(), init)?;
        deadline_after(deadline, period)?;
        Ok(Self {
            interval: None,
            deadline,
            period,
            timer_cancel: CancellationToken::new(),
            owner_cancel,
        })
    }
    /// Waits for the next absolute tick using a Tokio time driver; missed ticks use Skip.
    /// Explicit or owner cancellation returns `TimerCancelled` and takes priority
    /// when both cancellation and expiration are observed ready.
    pub async fn tick(&mut self) -> Result<()> {
        if self.is_cancelled() {
            return Err(VertxError::TimerCancelled);
        }
        let interval = self.interval.get_or_insert_with(|| {
            let mut iv = tokio::time::interval_at(self.deadline, self.period);
            iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            iv
        });
        tokio::select! { biased;
            _ = self.timer_cancel.cancelled() => Err(VertxError::TimerCancelled),
            _ = self.owner_cancel.cancelled() => Err(VertxError::TimerCancelled),
            _ = interval.tick() => Ok(()),
        }
    }
    /// Cancels this interval and wakes any pending tick.
    pub fn cancel(&self) {
        self.timer_cancel.cancel();
    }
    /// Reports whether this interval or its owner has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.timer_cancel.is_cancelled() || self.owner_cancel.is_cancelled()
    }
}

fn deadline_after(base: Instant, duration: Duration) -> Result<Instant> {
    base.checked_add(duration)
        .ok_or_else(|| VertxError::InvalidOptions("timer deadline is out of range".into()))
}
