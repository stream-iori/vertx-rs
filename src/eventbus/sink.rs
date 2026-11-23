use super::envelope::{ErasedEnvelope, MessageTrace};
use super::queue::Reservation;
use super::route::Router;
use super::{DeliveryOptions, EventBus, Headers};
use crate::{Result, VertxError};
use futures::Sink;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

impl EventBus {
    /// Creates a send-only Sink adapter; creation does not require a live consumer.
    /// Readiness reserves a selected consumer slot. Flush confirms admission only;
    /// close/Drop releases reservations without closing the shared address.
    pub fn sink<T: Send + 'static>(&self, address: &str) -> Result<MessageSink<T>> {
        self.sink_with(address, DeliveryOptions::default())
    }

    /// Configures headers for send-only messages. The timeout option, as with
    /// send_with, applies only to request APIs and is not an admission deadline.
    pub fn sink_with<T: Send + 'static>(
        &self,
        address: &str,
        options: DeliveryOptions,
    ) -> Result<MessageSink<T>> {
        self.ensure_open()?;
        Ok(MessageSink {
            router: self.router.clone(),
            address: address.into(),
            headers: options.take_headers(),
            pending: None,
            closed: false,
            marker: PhantomData,
        })
    }
}

/// Address-bound, send-only sink. Readiness holds one slot in the selected
/// consumer. A successful send/flush means admission, not business completion.
/// Close and Drop affect only this adapter, never the shared EventBus address.
pub struct MessageSink<T> {
    router: Arc<Router>,
    address: String,
    headers: Headers,
    pending: Option<PendingSend>,
    closed: bool,
    marker: PhantomData<fn() -> T>,
}

struct PendingSend {
    reservation: Reservation,
    trace: MessageTrace,
}

impl<T: Send + 'static> MessageSink<T> {
    fn pending_send(&mut self) -> Result<&mut PendingSend> {
        if self.pending.is_none() {
            let sender = self.router.select::<T>(&self.address)?;
            let trace = MessageTrace::new(&self.router.diagnostics, &self.address, None);
            self.pending = Some(PendingSend {
                reservation: sender.reserve(),
                trace,
            });
        }
        Ok(self.pending.as_mut().expect("sink owns a pending send"))
    }

    fn close_adapter(&mut self) {
        self.closed = true;
        self.pending.take();
    }

    fn closed_error(&self) -> VertxError {
        VertxError::ChannelClosed(self.address.clone())
    }
}

impl<T: Send + 'static> Sink<T> for MessageSink<T> {
    type Error = VertxError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = self.get_mut();
        if this.closed {
            return Poll::Ready(Err(this.closed_error()));
        }
        let result = match this.pending_send() {
            Ok(send) => send.reservation.poll_ready(cx),
            Err(error) => Poll::Ready(Err(error)),
        };
        if matches!(&result, Poll::Ready(Err(_))) {
            this.close_adapter();
        }
        result
    }

    fn start_send(self: Pin<&mut Self>, body: T) -> Result<()> {
        let this = self.get_mut();
        if this.closed {
            return Err(this.closed_error());
        }
        let PendingSend { reservation, trace } = this
            .pending
            .take()
            .ok_or_else(|| VertxError::Other("poll_ready must succeed before start_send".into()))?;
        let result = reservation.send(ErasedEnvelope {
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

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<()>> {
        // start_send commits immediately; this is not a remote-processing ack.
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<()>> {
        self.get_mut().close_adapter();
        Poll::Ready(Ok(()))
    }
}
