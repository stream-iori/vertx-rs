use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use tokio::sync::mpsc;

use super::envelope::{Envelope, ErasedEnvelope};
use super::inner::{EventBusInner, SubscriberId};
use crate::Context;

/// Typed consumer for one address. Dropping unregisters from the bus.
pub struct Consumer<T> {
    rx: mpsc::Receiver<ErasedEnvelope>,
    address: String,
    id: SubscriberId,
    bus: Arc<EventBusInner>,
    _marker: PhantomData<T>,
}

impl<T: Send + 'static> Consumer<T> {
    pub(crate) fn new(
        rx: mpsc::Receiver<ErasedEnvelope>,
        address: String,
        id: SubscriberId,
        bus: Arc<EventBusInner>,
    ) -> Self {
        Self { rx, address, id, bus, _marker: PhantomData }
    }

    /// Await the next message. Returns `None` once the consumer is unregistered.
    pub async fn recv(&mut self) -> Option<Envelope<T>> {
        loop {
            let erased = self.rx.recv().await?;
            let ErasedEnvelope { body, headers, reply_to } = erased;
            match body.downcast::<T>() {
                Ok(b) => return Some(Envelope::new(*b, headers, reply_to)),
                Err(_) => {
                    tracing::error!(
                        address = %self.address,
                        "dropping message: type mismatch on consumer"
                    );
                    continue;
                }
            }
        }
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub(crate) fn subscriber_id(&self) -> SubscriberId {
        self.id
    }

    /// Vert.x-style handler attachment. Consumes the `Consumer` and spawns a
    /// task on the current EventLoop that pulls envelopes and runs `handler`
    /// for each one. Equivalent to:
    ///
    /// ```ignore
    /// while let Some(env) = consumer.recv().await {
    ///     handler(env).await;
    /// }
    /// ```
    ///
    /// Must be called on an EventLoop context (the same one that owns the
    /// Consumer). Termination: the spawned task ends when the consumer is
    /// unregistered (e.g. on `undeploy`) — `recv()` returns `None` once the
    /// bus drops the sender side.
    ///
    /// Returns immediately. If a handler errors, the error is logged and the
    /// loop continues (so one bad message can't kill the subscription).
    ///
    /// ## Where to keep state
    ///
    /// Pick the *least* powerful tool that works — going further down this
    /// list trades compile-time guarantees for runtime checks.
    ///
    /// 1. **Single handler, owned state** — just `move` it into the closure
    ///    and use `FnMut`. Zero overhead, fully compile-checked.
    ///    ```ignore
    ///    let mut count: u64 = 0;
    ///    consumer.handler(move |env| {
    ///        count += 1;
    ///        async move { env.reply(count) }
    ///    });
    ///    ```
    /// 2. **Multiple handlers sharing a small `Copy` scalar** — `Rc<Cell<T>>`.
    ///    No borrow guard, no panic possible.
    /// 3. **Multiple handlers sharing complex state** (`HashMap`, `Vec`, ...)
    ///    — `Rc<RefCell<T>>`. **Rule:** keep every `borrow()`/`borrow_mut()`
    ///    scope to a single statement, and never hold a borrow across an
    ///    `.await`. Violating either invariant turns into a runtime panic
    ///    (`already borrowed`).
    /// 4. **Cross-handler coordination that doesn't fit shared state** —
    ///    route a message through another EventBus address. No shared mutability.
    pub fn handler<F, Fut>(self, handler: F)
    where
        F: FnMut(Envelope<T>) -> Fut + 'static,
        Fut: Future<Output = crate::Result<()>> + 'static,
    {
        let ctx = Context::current().expect(
            "Consumer::handler must be called on a Vertx EventLoop context",
        );
        let address = self.address.clone();
        let mut consumer = self;
        let mut handler = handler;
        let err_ctx = ctx.clone();
        ctx.spawn_local(async move {
            while let Some(env) = consumer.recv().await {
                if let Err(e) = handler(env).await {
                    tracing::error!(%address, error = %e, "handler returned error");
                    err_ctx.report_error(&e);
                }
            }
        });
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.bus.unregister(&self.address, self.id);
    }
}
