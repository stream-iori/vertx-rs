//! Timer and Periodic — Vert.x-style timer API.
//!
//! - `Timer` is one-shot, implements `Future<Output = Result<()>>`. `cancel()`
//!   makes the future resolve with `VertxError::TimerCancelled`.
//! - `Periodic` is repeated. Call `tick().await` for each fire. `cancel()` is
//!   the same as for `Timer`.
//!
//! A timer obtained from `Vertx::timer` / `Vertx::periodic` is **unbound** —
//! it lives until cancelled or completed. A timer obtained from
//! `Context::timer` / `Context::periodic` is **bound to the verticle's
//! deployment** — undeploy will cancel it automatically, just like a bound
//! `EventBus::consumer`.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context as StdContext, Poll};
use std::time::Duration;

use tokio::time::{interval, Instant, Interval};
use tokio_util::sync::CancellationToken;

use crate::{Result, VertxError};

/// A one-shot timer. Awaiting yields `Ok(())` when the deadline fires, or
/// `Err(VertxError::TimerCancelled)` if it was cancelled (explicitly or
/// because the owning Verticle was undeployed).
pub struct Timer {
    fut: Pin<Box<dyn Future<Output = Result<()>> + Send>>,
    own_cancel: CancellationToken,
}

impl Timer {
    pub(crate) fn new(delay: Duration, scope_cancel: Option<CancellationToken>) -> Self {
        let own_cancel = CancellationToken::new();
        let own_clone = own_cancel.clone();
        let fut = Box::pin(async move {
            let cancelled = async {
                if let Some(c) = scope_cancel {
                    c.cancelled().await;
                } else {
                    std::future::pending::<()>().await;
                }
            };
            tokio::select! {
                _ = tokio::time::sleep(delay) => Ok(()),
                _ = own_clone.cancelled() => Err(VertxError::TimerCancelled),
                _ = cancelled => Err(VertxError::TimerCancelled),
            }
        });
        Self { fut, own_cancel }
    }

    /// Cancel the timer. Subsequent (or in-flight) awaits resolve to
    /// `Err(VertxError::TimerCancelled)`.
    pub fn cancel(&self) {
        self.own_cancel.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.own_cancel.is_cancelled()
    }
}

impl Future for Timer {
    type Output = Result<()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut StdContext<'_>) -> Poll<Self::Output> {
        self.fut.as_mut().poll(cx)
    }
}

/// A periodic timer. `tick().await` resolves once per period, until either
/// `cancel()` is called or the owning Verticle is undeployed.
pub struct Periodic {
    interval: Interval,
    own_cancel: CancellationToken,
    scope_cancel: Option<CancellationToken>,
    cancelled: bool,
}

impl Periodic {
    pub(crate) fn new(
        initial_delay: Duration,
        period: Duration,
        scope_cancel: Option<CancellationToken>,
    ) -> Self {
        let mut iv = interval(period);
        iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Push the first tick out by `initial_delay`.
        iv.reset_at(Instant::now() + initial_delay);
        Self {
            interval: iv,
            own_cancel: CancellationToken::new(),
            scope_cancel,
            cancelled: false,
        }
    }

    /// Wait for the next tick. Returns `Err(TimerCancelled)` after the timer
    /// has been cancelled (explicitly or by undeploy of the owning Verticle).
    pub async fn tick(&mut self) -> Result<()> {
        if self.cancelled {
            return Err(VertxError::TimerCancelled);
        }
        let scope_cancel = self.scope_cancel.clone();
        let own_cancel = self.own_cancel.clone();
        let fired = tokio::select! {
            _ = self.interval.tick() => true,
            _ = own_cancel.cancelled() => false,
            _ = async {
                if let Some(c) = scope_cancel {
                    c.cancelled().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => false,
        };
        if fired {
            Ok(())
        } else {
            self.cancelled = true;
            Err(VertxError::TimerCancelled)
        }
    }

    pub fn cancel(&self) {
        self.own_cancel.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.own_cancel.is_cancelled()
    }
}
