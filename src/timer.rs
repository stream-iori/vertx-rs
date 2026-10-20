use crate::{Result, VertxError};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::{Instant, Interval};
use tokio_util::sync::CancellationToken;

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
        let state = Arc::new(AtomicU8::new(0));
        let (own_c, scope_c, state_c) = (own.clone(), scope.clone(), state.clone());
        let future = Box::pin(async move {
            let result = tokio::select! { biased;
                _ = own_c.cancelled() => Err(VertxError::TimerCancelled),
                _ = scope_c.cancelled() => Err(VertxError::TimerCancelled),
                _ = tokio::time::sleep_until(deadline) => Ok(()),
            };
            state_c.store(if result.is_ok() { 1 } else { 2 }, Ordering::Release);
            result
        });
        Self {
            future,
            own,
            scope,
            state,
        }
    }
    pub fn cancel(&self) {
        if self
            .state
            .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.own.cancel();
        }
    }
    pub fn is_cancelled(&self) -> bool {
        self.state.load(Ordering::Acquire) == 2
            || self.state.load(Ordering::Acquire) == 0 && self.scope.is_cancelled()
    }
}
impl Future for Timer {
    type Output = Result<()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.future.as_mut().poll(cx)
    }
}
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
