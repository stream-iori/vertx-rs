use super::{DeliveryOptions, Headers};
use crate::{Result, VertxError};
use std::any::Any;
use tokio::sync::oneshot;

pub(crate) fn next_message_id() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}
#[derive(Clone)]
pub(crate) struct MessageTrace {
    pub id: u64,
    pub request_id: Option<u64>,
    pub span: tracing::Span,
    pub dispatch: tracing::Dispatch,
    address: std::sync::Arc<str>,
    instance: u64,
    enabled: bool,
    created: std::time::Instant,
    enqueued: Option<std::time::Instant>,
}
impl MessageTrace {
    pub fn new(
        diagnostics: &crate::diagnostics::Diagnostics,
        address: &str,
        request_id: Option<u64>,
    ) -> Self {
        let id = request_id.unwrap_or_else(next_message_id);
        Self {
            id,
            request_id,
            span: if diagnostics.options.enabled {
                tracing::debug_span!(
                    "message",
                    instance_id = diagnostics.instance,
                    message_id = id,
                    ?request_id,
                    address
                )
            } else {
                tracing::Span::none()
            },
            dispatch: tracing::dispatcher::get_default(Clone::clone),
            address: address.into(),
            instance: diagnostics.instance,
            enabled: diagnostics.options.enabled,
            created: std::time::Instant::now(),
            enqueued: None,
        }
    }
    pub fn handler_span(&self, ctx: &crate::Context) -> tracing::Span {
        if !self.enabled {
            return tracing::Span::none();
        }
        let task_id = ctx
            .inner
            .diagnostics
            .current_task_id()
            .expect("handler polls inside a managed task operation");
        tracing::dispatcher::with_default(&self.dispatch, || {
            // The message's parent may describe its sender. Name the receiving
            // execution explicitly so causal ancestry cannot masquerade as CURRENT.
            tracing::debug_span!(parent: &self.span, "message_handler", instance_id = self.instance,
                deployment_id = %ctx.scope.deployment, context_id = %ctx.id(), task_id = %task_id)
        })
    }
    pub fn mark_enqueued(&mut self) -> Option<Self> {
        self.enqueued = Some(std::time::Instant::now());
        self.enabled.then(|| self.clone())
    }
    pub fn log_enqueued(&self) {
        let _dispatch = tracing::dispatcher::set_default(&self.dispatch);
        let _span = self.span.enter();
        tracing::trace!(instance_id = self.instance, message_id = self.id, request_id = ?self.request_id, address = %self.address, admission_wait = ?self.enqueued.map(|at| at.duration_since(self.created)), "message enqueued");
    }
    pub fn dequeue(&self) {
        if !self.enabled {
            return;
        }
        let _dispatch = tracing::dispatcher::set_default(&self.dispatch);
        let _span = self.span.enter();
        tracing::trace!(instance_id = self.instance, message_id = self.id, request_id = ?self.request_id, address = %self.address, queue_residence = ?self.enqueued.map(|at| at.elapsed()), "message dequeued");
    }
    pub fn reply(&self, outcome: &'static str) {
        if !self.enabled {
            return;
        }
        let _dispatch = tracing::dispatcher::set_default(&self.dispatch);
        let _span = self.span.enter();
        tracing::debug!(instance_id = self.instance, message_id = self.id, request_id = ?self.request_id, address = %self.address, outcome, "request reply");
    }
}
pub(crate) struct ErasedEnvelope {
    pub trace: MessageTrace,
    pub body: Box<dyn Any + Send>,
    pub headers: Headers,
    pub reply_to: Option<oneshot::Sender<Result<ErasedEnvelope>>>,
}
impl ErasedEnvelope {
    pub fn typed<T: Send + 'static>(self, address: &str) -> Result<Envelope<T>> {
        let body = *self
            .body
            .downcast::<T>()
            .map_err(|_| VertxError::TypeMismatch(address.into()))?;
        Ok(Envelope {
            body,
            headers: self.headers,
            reply: self.reply_to.map(|tx| ReplyHandle {
                tx,
                trace: self.trace.clone(),
            }),
            trace: self.trace,
        })
    }
}
/// One-shot reply capability, movable across tasks. Dropping it closes a request.
pub struct ReplyHandle {
    trace: MessageTrace,
    tx: oneshot::Sender<Result<ErasedEnvelope>>,
}
impl ReplyHandle {
    pub fn reply<R: Send + 'static>(self, response: R) -> Result<()> {
        self.reply_with(response, DeliveryOptions::new())
    }
    pub fn reply_with<R: Send + 'static>(
        self,
        response: R,
        options: DeliveryOptions,
    ) -> Result<()> {
        let result = self
            .tx
            .send(Ok(ErasedEnvelope {
                trace: self.trace.clone(),
                body: Box::new(response),
                headers: options.take_headers(),
                reply_to: None,
            }))
            .map_err(|_| VertxError::ReplyClosed);
        self.trace.reply(if result.is_ok() {
            "sent"
        } else {
            "reply_closed"
        });
        result
    }
    pub fn fail(self, code: i32, message: impl Into<String>) -> Result<()> {
        let result = self
            .tx
            .send(Err(VertxError::RecipientFailure {
                code,
                message: message.into(),
            }))
            .map_err(|_| VertxError::ReplyClosed);
        self.trace.reply(if result.is_ok() {
            "recipient_failure"
        } else {
            "reply_closed"
        });
        result
    }
}
pub struct Envelope<T> {
    pub(crate) trace: MessageTrace,
    body: T,
    headers: Headers,
    reply: Option<ReplyHandle>,
}
impl<T> Envelope<T> {
    pub fn body(&self) -> &T {
        &self.body
    }
    pub fn body_mut(&mut self) -> &mut T {
        &mut self.body
    }
    pub fn headers(&self) -> &Headers {
        &self.headers
    }
    pub fn has_reply(&self) -> bool {
        self.reply.is_some()
    }
    pub fn reply<R: Send + 'static>(self, response: R) -> Result<()> {
        self.reply_handle()?.reply(response)
    }
    pub fn reply_with<R: Send + 'static>(
        self,
        response: R,
        options: DeliveryOptions,
    ) -> Result<()> {
        self.reply_handle()?.reply_with(response, options)
    }
    pub fn fail(self, code: i32, message: impl Into<String>) -> Result<()> {
        self.reply_handle()?.fail(code, message)
    }
    fn reply_handle(self) -> Result<ReplyHandle> {
        self.reply
            .ok_or_else(|| VertxError::Other("envelope has no reply channel".into()))
    }
    pub fn into_parts(self) -> (T, Headers, Option<ReplyHandle>) {
        (self.body, self.headers, self.reply)
    }
    pub fn into_body(self) -> T {
        self.body
    }
}
