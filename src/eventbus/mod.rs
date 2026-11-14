mod consumer;
mod envelope;
mod inner;
mod options;
use crate::deployment::DeploymentScope;
use crate::{Context, Result, VertxError};
pub use consumer::Consumer;
pub use envelope::{Envelope, ReplyHandle};
use inner::EventBusInner;
pub use options::{ConsumerOptions, DeliveryOptions, Headers};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Local typed address router. Context handles bind registration to a deployment;
/// Instance handles bind registration to a matching current Context when present.
/// Send awaits capacity; publish drops on full queues.
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
    /// Attempts delivery to all subscribers; full-queue deliveries are dropped.
    /// Success does not confirm business processing. Each message is cloned only
    /// after admission; routing and lifecycle errors are returned.
    pub fn publish<T: Clone + Send + 'static>(&self, address: &str, body: T) -> Result<()> {
        self.publish_with(address, body, DeliveryOptions::new())
    }
    /// Broadcasts with headers using the same drop policy as [`Self::publish`].
    pub fn publish_with<T: Clone + Send + 'static>(
        &self,
        address: &str,
        body: T,
        options: DeliveryOptions,
    ) -> Result<()> {
        self.inner.publish(address, body, options.take_headers())
    }
    /// Selects one consumer round-robin and awaits queue capacity.
    /// Success means enqueued, not processed. Cancelling does not retract queued
    /// messages; close/consumer removal unblocks capacity waits.
    pub async fn send<T: Send + 'static>(&self, address: &str, body: T) -> Result<()> {
        self.send_with(address, body, DeliveryOptions::new()).await
    }
    /// Sends with headers using [`Self::send`] semantics.
    /// The timeout option applies to request APIs, not as a queue deadline.
    pub async fn send_with<T: Send + 'static>(
        &self,
        address: &str,
        body: T,
        options: DeliveryOptions,
    ) -> Result<()> {
        self.inner.send(address, body, options.take_headers()).await
    }
    /// Attempts immediate admission to one consumer.
    /// Distinguishes `QueueFull`, `ChannelClosed`, `NoConsumer` and `TypeMismatch`.
    pub fn try_send<T: Send + 'static>(&self, address: &str, body: T) -> Result<()> {
        self.try_send_with(address, body, DeliveryOptions::new())
    }
    /// Attempts immediate admission with headers; see [`Self::try_send`].
    pub fn try_send_with<T: Send + 'static>(
        &self,
        address: &str,
        body: T,
        options: DeliveryOptions,
    ) -> Result<()> {
        self.inner.try_send(address, body, options.take_headers())
    }
    /// Waits for a typed reply; one default 30-second timeout covers admission and reply.
    /// Timeout/cancellation does not retract a queued message; late replies return
    /// `ReplyClosed`. Business failures remain `RecipientFailure`.
    pub async fn request<T: Send + 'static, R: Send + 'static>(
        &self,
        address: &str,
        body: T,
    ) -> Result<R> {
        self.request_with(address, body, DeliveryOptions::new())
            .await
    }
    /// Requests a typed body with headers and a timeout covering queueing plus reply.
    /// Reply headers are discarded; use [`Self::request_message_with`] to retain them.
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
    /// Requests a full reply envelope using the default 30-second timeout.
    /// The deadline includes queue admission; queued messages are not retracted.
    pub async fn request_message<T: Send + 'static, R: Send + 'static>(
        &self,
        address: &str,
        body: T,
    ) -> Result<Envelope<R>> {
        self.request_message_with(address, body, DeliveryOptions::new())
            .await
    }
    /// Requests a full response with explicit headers and timeout.
    /// The timeout covers admission and reply; lifecycle, routing, reply, type and
    /// recipient failures retain their distinct error variants.
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
    /// Creates a send-only Sink adapter; creation does not require a live consumer.
    /// Readiness reserves a selected consumer slot. Flush confirms admission only;
    /// close/Drop releases reservations without closing the shared address.
    pub fn sink<T: Send + 'static>(&self, address: &str) -> Result<MessageSink<T>> {
        self.sink_with(address, DeliveryOptions::default())
    }
    /// Configure headers for send-only messages. The timeout option, as with
    /// send_with, applies only to request APIs and is not an admission deadline.
    pub fn sink_with<T: Send + 'static>(
        &self,
        address: &str,
        options: DeliveryOptions,
    ) -> Result<MessageSink<T>> {
        self.ensure_open()?;
        Ok(MessageSink {
            bus: self.inner.clone(),
            address: address.into(),
            headers: options.take_headers(),
            reservation: None,
            trace: None,
            closed: false,
            marker: std::marker::PhantomData,
        })
    }
    /// Immediately registers with capacity 256. A live address with a different
    /// body type returns `TypeMismatch`. Registration binds to the bus Context, or
    /// this instance's current Context; otherwise it is unbound and supports pull
    /// receiving. Dropping the consumer unregisters it.
    pub fn consumer<T: Send + 'static>(&self, address: &str) -> Result<Consumer<T>> {
        self.consumer_with(address, ConsumerOptions::default())
    }
    /// Registers with capacity in `1..=Semaphore::MAX_PERMITS`, measured in messages.
    /// Invalid capacity returns `InvalidOptions`. Registration rejects closed scopes
    /// or instances and is atomic with scope admission; see [`Self::consumer`].
    pub fn consumer_with<T: Send + 'static>(
        &self,
        address: &str,
        options: ConsumerOptions,
    ) -> Result<Consumer<T>> {
        options.validate()?;
        self.ensure_open()?;
        let owner = self.owner.clone().or_else(|| {
            Context::current().filter(|c| Arc::ptr_eq(&c.inner.event_bus.inner, &self.inner))
        });
        if let Some(ctx) = &owner {
            let mut state = ctx.scope.admit()?;
            let consumer = self
                .inner
                .register(address, Some(ctx.clone()), options.capacity())?;
            state
                .subscribers
                .push((address.into(), consumer.subscriber_id()));
            Ok(consumer)
        } else {
            self.inner.register(address, None, options.capacity())
        }
    }
}

/// Address-bound, send-only sink. Readiness holds one slot in the selected
/// consumer. A successful send/flush means admission, not business completion.
/// Close and Drop affect only this adapter, never the shared EventBus address.
pub struct MessageSink<T> {
    bus: Arc<EventBusInner>,
    address: String,
    headers: Headers,
    reservation: Option<inner::Reservation>,
    trace: Option<envelope::MessageTrace>,
    closed: bool,
    marker: std::marker::PhantomData<fn() -> T>,
}
impl<T> MessageSink<T> {
    fn close_adapter(&mut self) {
        self.closed = true;
        self.reservation.take();
        self.trace.take();
    }
}
impl<T: Send + 'static> futures::Sink<T> for MessageSink<T> {
    type Error = VertxError;
    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>> {
        use std::task::Poll;
        let this = self.get_mut();
        if this.closed {
            return Poll::Ready(Err(VertxError::ChannelClosed(this.address.clone())));
        }
        if this.reservation.is_none() {
            match this.bus.select::<T>(&this.address) {
                Ok(target) => {
                    this.trace = Some(envelope::MessageTrace::new(
                        &this.bus.diagnostics,
                        &this.address,
                        None,
                    ));
                    this.reservation = Some(target.reserve());
                }
                Err(error) => {
                    this.close_adapter();
                    return Poll::Ready(Err(error));
                }
            }
        }
        let result = this
            .reservation
            .as_mut()
            .expect("reservation was selected above")
            .poll_ready(cx);
        if matches!(&result, Poll::Ready(Err(_))) {
            this.close_adapter();
        }
        result
    }
    fn start_send(self: std::pin::Pin<&mut Self>, body: T) -> Result<()> {
        let this = self.get_mut();
        if this.closed {
            return Err(VertxError::ChannelClosed(this.address.clone()));
        }
        let reservation = this
            .reservation
            .take()
            .ok_or_else(|| VertxError::Other("poll_ready must succeed before start_send".into()))?;
        let trace = this
            .trace
            .take()
            .expect("each reservation owns a message trace");
        let result = reservation.send(envelope::ErasedEnvelope {
            trace,
            body: Box::new(body),
            headers: this.headers.clone(),
            reply_to: None,
        });
        if result.is_err() {
            this.close_adapter();
        }
        result
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>> {
        // start_send commits immediately; this is not a remote-processing ack.
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>> {
        self.get_mut().close_adapter();
        std::task::Poll::Ready(Ok(()))
    }
}
