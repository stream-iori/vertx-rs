//! `DeliveryOptions` — counterpart to `io.vertx.core.eventbus.DeliveryOptions`.
//!
//! Carries per-call delivery configuration for `EventBus::send` /
//! `EventBus::request` / `EventBus::publish`. Today only `timeout` is honored;
//! `headers` is reserved for future codec / interceptor support.

use std::time::Duration;

/// Vert.x default request timeout: 30 seconds.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Per-call delivery configuration.
#[derive(Debug, Clone)]
pub struct DeliveryOptions {
    timeout: Duration,
}

impl Default for DeliveryOptions {
    fn default() -> Self {
        Self { timeout: DEFAULT_TIMEOUT }
    }
}

impl DeliveryOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the request/response timeout. Mirrors
    /// `DeliveryOptions.setSendTimeout(long)`.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}
