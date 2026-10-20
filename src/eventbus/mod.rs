mod consumer;
mod envelope;
mod inner;
mod options;
use crate::deployment::DeploymentScope;
use crate::{Context, Result, VertxError};
pub use consumer::Consumer;
pub use envelope::{Envelope, ReplyHandle};
use inner::EventBusInner;
pub use options::{DeliveryOptions, Headers};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Local typed address router. Context handles bind registration to a deployment;
/// Vertx handles are unbound. Send awaits capacity; publish drops on full queues.
#[derive(Clone)]
pub struct EventBus {
    pub(crate) inner: Arc<EventBusInner>,
    owner: Option<Context>,
}
impl EventBus {
    pub(crate) fn new(
        cancel: CancellationToken,
        diagnostics: Arc<crate::diagnostics::Diagnostics>,
    ) -> Self {
        Self {
            inner: Arc::new(EventBusInner::new(cancel, diagnostics)),
            owner: None,
        }
    }
    pub(crate) fn bound(&self, ctx: Context) -> Self {
        Self {
            inner: self.inner.clone(),
            owner: Some(ctx),
        }
    }
    pub(crate) fn ensure_open(&self) -> Result<()> {
        self.inner.ensure_open()
    }
    pub(crate) fn close(&self) {
        self.inner.close();
    }
    pub(crate) fn cleanup_scope(&self, scope: &DeploymentScope) {
        let subscribers = std::mem::take(&mut scope.state.lock().unwrap().subscribers);
        for (address, id) in subscribers {
            self.inner.unregister(&address, id);
        }
    }
    pub fn publish<T: Clone + Send + 'static>(&self, address: &str, body: T) -> Result<()> {
        self.publish_with(address, body, DeliveryOptions::new())
    }
    pub fn publish_with<T: Clone + Send + 'static>(
        &self,
        address: &str,
        body: T,
        options: DeliveryOptions,
    ) -> Result<()> {
        self.inner.publish(address, body, options.take_headers())
    }
    pub async fn send<T: Send + 'static>(&self, address: &str, body: T) -> Result<()> {
        self.send_with(address, body, DeliveryOptions::new()).await
    }
    pub async fn send_with<T: Send + 'static>(
        &self,
        address: &str,
        body: T,
        options: DeliveryOptions,
    ) -> Result<()> {
        self.inner.send(address, body, options.take_headers()).await
    }
    pub fn try_send<T: Send + 'static>(&self, address: &str, body: T) -> Result<()> {
        self.try_send_with(address, body, DeliveryOptions::new())
    }
    pub fn try_send_with<T: Send + 'static>(
        &self,
        address: &str,
        body: T,
        options: DeliveryOptions,
    ) -> Result<()> {
        self.inner.try_send(address, body, options.take_headers())
    }
    pub async fn request<T: Send + 'static, R: Send + 'static>(
        &self,
        address: &str,
        body: T,
    ) -> Result<R> {
        self.request_with(address, body, DeliveryOptions::new())
            .await
    }
    pub async fn request_with<T: Send + 'static, R: Send + 'static>(
        &self,
        address: &str,
        body: T,
        options: DeliveryOptions,
    ) -> Result<R> {
        Ok(self
            .request_message_with::<T, R>(address, body, options)
            .await?
            .into_body())
    }
    pub async fn request_message<T: Send + 'static, R: Send + 'static>(
        &self,
        address: &str,
        body: T,
    ) -> Result<Envelope<R>> {
        self.request_message_with(address, body, DeliveryOptions::new())
            .await
    }
    pub async fn request_message_with<T: Send + 'static, R: Send + 'static>(
        &self,
        address: &str,
        body: T,
        options: DeliveryOptions,
    ) -> Result<Envelope<R>> {
        let request_id = envelope::next_message_id();
        let timeout = options.timeout();
        let request = async {
            let rx = self
                .inner
                .send_with_reply(request_id, address, body, options.take_headers())
                .await?;
            let response = tokio::select! { biased;
                _ = self.inner.closed.cancelled() => return Err(VertxError::VertxClosed),
                r = rx => r.map_err(|_| VertxError::ReplyClosed)??,
            };
            response.trace.reply("received");
            response.typed(address)
        };
        tokio::time::timeout(timeout, request).await.map_err(|_| {
            self.inner.diagnostics.timeout();
            if self.inner.diagnostics.options.enabled {
                tracing::warn!(
                    instance_id = self.inner.diagnostics.instance,
                    address,
                    request_id,
                    error_kind = "timeout",
                    ?timeout,
                    "request timed out"
                );
            }
            VertxError::Timeout(timeout)
        })?
    }
    pub fn consumer<T: Send + 'static>(&self, address: &str) -> Result<Consumer<T>> {
        self.ensure_open()?;
        let owner = self.owner.clone().or_else(|| {
            Context::current().filter(|c| Arc::ptr_eq(&c.inner.event_bus.inner, &self.inner))
        });
        if let Some(ctx) = &owner {
            let mut state = ctx.scope.admit()?;
            let consumer = self.inner.register(address, Some(ctx.clone()))?;
            state
                .subscribers
                .push((address.into(), consumer.subscriber_id()));
            Ok(consumer)
        } else {
            self.inner.register(address, None)
        }
    }
}
