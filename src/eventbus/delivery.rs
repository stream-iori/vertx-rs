use super::envelope::{self, ErasedEnvelope, MessageTrace};
use super::queue::QueueSender;
use super::route::RouteKind;
use super::{DeliveryOptions, Envelope, EventBus, Headers, RequestOptions};
use crate::{Result, VertxError};
use tokio::sync::oneshot;

impl EventBus {
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
        let senders = self.router().subscribers::<T>(address)?;
        let headers = options.into_headers();
        let diagnostics = &self.router().diagnostics;
        let mut dropped = 0;
        for sender in senders {
            let Ok(reservation) = sender.try_reserve() else {
                dropped += 1;
                continue;
            };
            // Clone is user code. The slot is ready and the route lock is released.
            let envelope = ErasedEnvelope {
                trace: MessageTrace::new(diagnostics, address, None),
                body: Box::new(body.clone()),
                headers: headers.clone(),
                reply_to: None,
            };
            if reservation.send(envelope).is_err() {
                dropped += 1;
            }
        }
        if dropped > 0 && diagnostics.options.enabled {
            diagnostics.dropped(dropped);
            tracing::warn!(
                instance_id = diagnostics.instance,
                address,
                dropped,
                "broadcast delivery dropped"
            );
        }
        Ok(())
    }
    /// Selects one consumer round-robin and awaits queue capacity.
    /// Success means enqueued, not processed. Cancelling does not retract queued
    /// messages; close/consumer removal unblocks capacity waits.
    pub async fn send<T: Send + 'static>(&self, address: &str, body: T) -> Result<()> {
        self.send_with(address, body, DeliveryOptions::new()).await
    }
    /// Sends with headers using [`Self::send`] semantics.
    pub async fn send_with<T: Send + 'static>(
        &self,
        address: &str,
        body: T,
        options: DeliveryOptions,
    ) -> Result<()> {
        let (sender, envelope) = self.prepare(
            address,
            RouteKind::message::<T>(),
            body,
            options.into_headers(),
            None,
            None,
        )?;
        sender.send(envelope).await
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
        let (sender, envelope) = self.prepare(
            address,
            RouteKind::message::<T>(),
            body,
            options.into_headers(),
            None,
            None,
        )?;
        sender.try_reserve()?.send(envelope)
    }
    /// Waits for a typed reply; one default 30-second timeout covers admission and reply.
    /// Timeout/cancellation does not retract a queued message; late replies return
    /// `ReplyClosed`. Business failures remain `RecipientFailure`.
    pub async fn request<T: Send + 'static, R: Send + 'static>(
        &self,
        address: &str,
        body: T,
    ) -> Result<R> {
        self.request_with(address, body, RequestOptions::new())
            .await
    }
    /// Requests a typed body with headers and a timeout covering queueing plus reply.
    /// Reply headers are discarded; use [`Self::request_message_with`] to retain them.
    pub async fn request_with<T: Send + 'static, R: Send + 'static>(
        &self,
        address: &str,
        body: T,
        options: RequestOptions,
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
        self.request_message_with(address, body, RequestOptions::new())
            .await
    }
    /// Requests a full response with explicit headers and timeout.
    /// The timeout covers admission and reply; lifecycle, routing, reply, type and
    /// recipient failures retain their distinct error variants.
    pub async fn request_message_with<T: Send + 'static, R: Send + 'static>(
        &self,
        address: &str,
        body: T,
        options: RequestOptions,
    ) -> Result<Envelope<R>> {
        self.request_for(address, RouteKind::message::<T>(), body, options)
            .await
    }

    pub(super) async fn request_for<T: Send + 'static, R: Send + 'static>(
        &self,
        address: &str,
        kind: RouteKind,
        body: T,
        options: RequestOptions,
    ) -> Result<Envelope<R>> {
        let request_id = envelope::next_message_id();
        let timeout = options.timeout();
        let request = self.request_reply(request_id, address, kind, body, options.into_headers());
        tokio::time::timeout(timeout, request).await.map_err(|_| {
            let diagnostics = &self.router().diagnostics;
            diagnostics.timeout();
            if diagnostics.options.enabled {
                tracing::warn!(
                    instance_id = diagnostics.instance,
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

    async fn request_reply<T: Send + 'static, R: Send + 'static>(
        &self,
        request_id: u64,
        address: &str,
        kind: RouteKind,
        body: T,
        headers: Headers,
    ) -> Result<Envelope<R>> {
        let (reply_to, reply) = oneshot::channel();
        let (sender, envelope) = self.prepare(
            address,
            kind,
            body,
            headers,
            Some(reply_to),
            Some(request_id),
        )?;
        sender.send(envelope).await?;
        let response = tokio::select! { biased;
            _ = self.router().closed.cancelled() => return Err(VertxError::VertxClosed),
            result = reply => result.map_err(|_| VertxError::ReplyClosed)??,
        };
        response.trace.reply("received");
        response.typed(address)
    }

    fn prepare<T: Send + 'static>(
        &self,
        address: &str,
        kind: RouteKind,
        body: T,
        headers: Headers,
        reply_to: Option<oneshot::Sender<Result<ErasedEnvelope>>>,
        request_id: Option<u64>,
    ) -> Result<(QueueSender, ErasedEnvelope)> {
        let sender = self.router().select(address, kind)?;
        let envelope = ErasedEnvelope {
            trace: MessageTrace::new(&self.router().diagnostics, address, request_id),
            body: Box::new(body),
            headers,
            reply_to,
        };
        Ok((sender, envelope))
    }
}
