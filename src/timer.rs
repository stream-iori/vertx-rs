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
    own: CancellationToken,
    scope: CancellationToken,
    state: Arc<AtomicU8>,
}
impl Timer {
    pub(crate) fn new(delay: Duration, scope: CancellationToken) -> Self {
        let deadline = Instant::now() + delay;
        let own = CancellationToken::new();
        let state = Arc::new(AtomicU8::new(PENDING));
        let (own_c, scope_c, state_c) = (own.clone(), scope.clone(), state.clone());
        let future = Box::pin(async move {
            let result = tokio::select! { biased;
                _ = own_c.cancelled() => Err(VertxError::TimerCancelled),
                _ = scope_c.cancelled() => Err(VertxError::TimerCancelled),
                _ = tokio::time::sleep_until(deadline) => Ok(()),
            };
            state_c.store(
                if result.is_ok() { COMPLETED } else { CANCELLED },
                Ordering::Release,
            );
            result
        });
        Self {
            future,
            own,
            scope,
            state,
        }
    }
    /// Cancels a pending timer without changing an already-completed timer.
    pub fn cancel(&self) {
        if self
            .state
            .compare_exchange(PENDING, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.own.cancel();
        }
    }
    /// Includes explicit and scope cancellation while pending.
    /// A completed timer does not become cancelled on later close.
    pub fn is_cancelled(&self) -> bool {
        self.state.load(Ordering::Acquire) == CANCELLED
            || self.state.load(Ordering::Acquire) == PENDING && self.scope.is_cancelled()
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
    own: CancellationToken,
    scope: CancellationToken,
}
impl Periodic {
    pub(crate) fn new(init: Duration, period: Duration, scope: CancellationToken) -> Result<Self> {
        if period.is_zero() {
            return Err(VertxError::InvalidOptions("period must be positive".into()));
        }
        Ok(Self {
            interval: None,
            deadline: Instant::now() + init,
            period,
            own: CancellationToken::new(),
            scope,
        })
    }
    /// Waits for the next absolute tick using a Tokio time driver; missed ticks use Skip.
    /// Explicit or scope cancellation returns `TimerCancelled` and takes priority
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
            _ = self.own.cancelled() => Err(VertxError::TimerCancelled),
            _ = self.scope.cancelled() => Err(VertxError::TimerCancelled),
            _ = interval.tick() => Ok(()),
        }
    }
    pub fn cancel(&self) {
        self.own.cancel();
    }
    pub fn is_cancelled(&self) -> bool {
        self.own.is_cancelled() || self.scope.is_cancelled()
    }
}
