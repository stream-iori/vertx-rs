use super::{DeliveryOptions, Headers};
use crate::{Result, VertxError};
use std::any::Any;
use tokio::sync::oneshot;

pub(crate) struct ErasedEnvelope {
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
            reply: self.reply_to.map(|tx| ReplyHandle { tx }),
        })
    }
}
/// One-shot reply capability, movable across tasks. Dropping it closes a request.
pub struct ReplyHandle {
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
        self.tx
            .send(Ok(ErasedEnvelope {
                body: Box::new(response),
                headers: options.take_headers(),
                reply_to: None,
            }))
            .map_err(|_| VertxError::ReplyClosed)
    }
    pub fn fail(self, code: i32, message: impl Into<String>) -> Result<()> {
        self.tx
            .send(Err(VertxError::RecipientFailure {
                code,
                message: message.into(),
            }))
            .map_err(|_| VertxError::ReplyClosed)
    }
}
pub struct Envelope<T> {
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
