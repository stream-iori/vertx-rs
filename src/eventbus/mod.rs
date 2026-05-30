mod consumer;
mod envelope;
mod inner;
mod options;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;

use crate::{DeploymentId, Result, VertxError};

pub use consumer::Consumer;
pub use envelope::{Envelope, ReplyHandle};
pub use options::DeliveryOptions;

use inner::{EventBusInner, SubscriberId};

/// Lifecycle state shared between a deployed Verticle and any Context handle
/// derived from it. Outlives the Verticle's `start`/`stop` so that callers
/// holding a Context clone can detect post-undeploy state.
pub(crate) struct DeploymentScope {
    pub(crate) deployment: DeploymentId,
    /// Flipped to false by `undeploy` before tearing down. All Vertx-driven
    /// schedulers must check this before doing work on behalf of the verticle.
    pub(crate) alive: AtomicBool,
    /// Cancellation signal for the verticle's main task and any cooperative
    /// children the user wires up via `Context::cancellation_token`.
    pub(crate) cancel: CancellationToken,
    pub(crate) subscribers: Mutex<Vec<(String, SubscriberId)>>,
}

impl DeploymentScope {
    pub(crate) fn new(deployment: DeploymentId, cancel: CancellationToken) -> Self {
        Self {
            deployment,
            alive: AtomicBool::new(true),
            cancel,
            subscribers: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    pub(crate) fn mark_dead(&self) {
        self.alive.store(false, Ordering::Release);
    }
}

/// Handle to the local event bus. Cheap to clone.
///
/// A handle obtained from `Context::event_bus()` is **bound** to a Verticle:
/// consumers registered through it are automatically unregistered when the
/// Verticle is undeployed. A handle obtained from `Vertx::event_bus()` is
/// **unbound** — its consumers live until their `Consumer<T>` is dropped.
#[derive(Clone)]
pub struct EventBus {
    inner: Arc<EventBusInner>,
    scope: Option<Arc<DeploymentScope>>,
}

impl EventBus {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(EventBusInner::new()),
            scope: None,
        }
    }

    /// Produce a bound handle for the given deployment scope.
    pub(crate) fn bound(&self, scope: Arc<DeploymentScope>) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            scope: Some(scope),
        }
    }

    /// Unregister all subscribers belonging to a scope.
    pub(crate) fn cleanup_scope(&self, scope: &DeploymentScope) {
        let mut subs = scope.subscribers.lock().unwrap();
        let n = subs.len();
        for (addr, id) in subs.drain(..) {
            self.inner.unregister(&addr, id);
        }
        tracing::debug!(
            deployment = %scope.deployment,
            unregistered = n,
            "cleaned up bound subscribers"
        );
    }

    /// Fire-and-forget broadcast: every subscriber on `address` receives a
    /// clone of `body`. Per-subscriber delivery is best-effort (drop-on-full).
    pub fn publish<T: Clone + Send + 'static>(&self, address: &str, body: T) -> Result<()> {
        self.inner.publish(address, body)
    }

    /// Point-to-point with cooperative backpressure.
    pub async fn send<T: Send + 'static>(&self, address: &str, body: T) -> Result<()> {
        self.inner.send(address, body).await
    }

    /// Non-blocking variant of `send`.
    pub fn try_send<T: Send + 'static>(&self, address: &str, body: T) -> Result<()> {
        self.inner.try_send(address, body)
    }

    /// Request/response with default `DeliveryOptions` (30s timeout).
    /// Mirrors `EventBus.request(address, message)`.
    pub async fn request<T, R>(&self, address: &str, body: T) -> Result<R>
    where
        T: Send + 'static,
        R: Send + 'static,
    {
        self.request_with(address, body, DeliveryOptions::default()).await
    }

    /// Request/response with explicit `DeliveryOptions`. Mirrors
    /// `EventBus.request(address, message, options)`.
    pub async fn request_with<T, R>(
        &self,
        address: &str,
        body: T,
        options: DeliveryOptions,
    ) -> Result<R>
    where
        T: Send + 'static,
        R: Send + 'static,
    {
        let rx = self.inner.send_with_reply(address, body).await?;
        let timeout = options.timeout();
        let erased = tokio::time::timeout(timeout, rx)
            .await
            .map_err(|_| VertxError::Timeout(timeout))?
            .map_err(|_| VertxError::ReplyClosed)?;
        erased
            .downcast::<R>()
            .map(|b| *b)
            .map_err(|_| VertxError::TypeMismatch(address.to_string()))
    }

    /// Register a typed consumer.
    ///
    /// For an **unbound** handle (from `Vertx::event_bus`) this always
    /// succeeds. For a **bound** handle (from `Context::event_bus`) returns
    /// `DeploymentTerminated` if the owning verticle has already been
    /// undeployed.
    pub fn consumer<T: Send + 'static>(&self, address: &str) -> Result<Consumer<T>> {
        if let Some(scope) = &self.scope {
            if !scope.is_alive() {
                return Err(VertxError::DeploymentTerminated(scope.deployment));
            }
        }
        let c = self.inner.register::<T>(address);
        if let Some(scope) = &self.scope {
            // Close the small race window between register and undeploy.
            if !scope.is_alive() {
                self.inner.unregister(address, c.subscriber_id());
                return Err(VertxError::DeploymentTerminated(scope.deployment));
            }
            scope
                .subscribers
                .lock()
                .unwrap()
                .push((address.to_string(), c.subscriber_id()));
        }
        Ok(c)
    }
}
