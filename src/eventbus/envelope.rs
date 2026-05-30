use std::any::Any;
use std::collections::HashMap;

use tokio::sync::oneshot;

use crate::{Result, VertxError};

/// Type-erased envelope flowing through the bus internals.
pub(crate) struct ErasedEnvelope {
    pub body: Box<dyn Any + Send>,
    pub headers: HashMap<String, String>,
    pub reply_to: Option<oneshot::Sender<Box<dyn Any + Send>>>,
}

/// A reply channel attached to an `Envelope` that was sent via `request`.
/// Decoupled from `Envelope` so callers can split an envelope into pieces
/// and reply later, possibly from a different task.
pub struct ReplyHandle {
    tx: oneshot::Sender<Box<dyn Any + Send>>,
}

impl ReplyHandle {
    pub(crate) fn new(tx: oneshot::Sender<Box<dyn Any + Send>>) -> Self {
        Self { tx }
    }

    /// Send a reply. Fails if the requester has already given up
    /// (e.g. their `request_with` timeout elapsed).
    pub fn reply<R: Send + 'static>(self, resp: R) -> Result<()> {
        self.tx
            .send(Box::new(resp))
            .map_err(|_| VertxError::ReplyClosed)
    }
}

/// Typed envelope handed to a `Consumer<T>`. Carries the message body,
/// transport headers and (for `request` messages) a reply handle.
///
/// Use `body()` / `headers()` / `reply_handle()` for read-only access, or
/// `into_parts()` to consume the envelope and destructure it.
pub struct Envelope<T> {
    body: T,
    headers: HashMap<String, String>,
    reply: Option<ReplyHandle>,
}

impl<T> Envelope<T> {
    pub(crate) fn new(
        body: T,
        headers: HashMap<String, String>,
        reply_to: Option<oneshot::Sender<Box<dyn Any + Send>>>,
    ) -> Self {
        Self {
            body,
            headers,
            reply: reply_to.map(ReplyHandle::new),
        }
    }

    pub fn body(&self) -> &T {
        &self.body
    }

    pub fn body_mut(&mut self) -> &mut T {
        &mut self.body
    }

    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }

    pub fn has_reply(&self) -> bool {
        self.reply.is_some()
    }

    /// Send a reply in-place, consuming the envelope. Convenience for the
    /// common case of handling a `request` and immediately replying.
    pub fn reply<R: Send + 'static>(self, resp: R) -> Result<()> {
        let handle = self.reply.ok_or_else(|| {
            VertxError::Other("envelope has no reply channel".into())
        })?;
        handle.reply(resp)
    }

    /// Consume the envelope and return its parts. Use this when you need
    /// to hand the reply handle to another task while keeping the body.
    pub fn into_parts(self) -> (T, HashMap<String, String>, Option<ReplyHandle>) {
        (self.body, self.headers, self.reply)
    }

    /// Consume the envelope and return just the body.
    pub fn into_body(self) -> T {
        self.body
    }
}
