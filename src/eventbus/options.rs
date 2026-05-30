//! `DeliveryOptions` — counterpart to `io.vertx.core.eventbus.DeliveryOptions`.
//!
//! Carries per-call delivery configuration for `EventBus::send` /
//! `EventBus::request` / `EventBus::publish`. Today honored: `timeout`
//! and `headers`. `codecName` / `localOnly` reserved for future use.

use std::collections::HashMap;
use std::time::Duration;

/// Vert.x default request timeout: 30 seconds.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Per-call delivery configuration.
#[derive(Debug, Clone, Default)]
pub struct DeliveryOptions {
    timeout: Option<Duration>,
    headers: HashMap<String, String>,
}

impl DeliveryOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the request/response timeout. Mirrors
    /// `DeliveryOptions.setSendTimeout(long)`.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Add a header. Mirrors `DeliveryOptions.addHeader(k, v)`.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Replace all headers.
    pub fn with_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers = headers;
        self
    }

    pub fn timeout(&self) -> Duration {
        self.timeout.unwrap_or(DEFAULT_TIMEOUT)
    }

    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }

    pub(crate) fn take_headers(self) -> HashMap<String, String> {
        self.headers
    }
}

