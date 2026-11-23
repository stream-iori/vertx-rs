mod consumer;
mod delivery;
mod envelope;
mod options;
mod queue;
mod route;
mod sink;

use crate::diagnostics::Diagnostics;
use crate::{Context, DeploymentId, Result};
pub use consumer::Consumer;
pub use envelope::{Envelope, ReplyHandle};
pub use options::{ConsumerOptions, DeliveryOptions, Headers};
use route::Router;
pub use sink::MessageSink;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Local typed address router. Context handles bind registration to a deployment;
/// Instance handles bind registration to a matching current Context when present.
/// Send awaits capacity; publish drops on full queues.
#[derive(Clone)]
pub struct EventBus {
    router: Arc<Router>,
    owner: Option<Context>,
}
impl EventBus {
    pub(crate) fn new(cancel: CancellationToken, diagnostics: Arc<Diagnostics>) -> Self {
        Self {
            router: Arc::new(Router::new(cancel, diagnostics)),
            owner: None,
        }
    }
    pub(crate) fn bound(&self, ctx: Context) -> Self {
        Self {
            router: self.router.clone(),
            owner: Some(ctx),
        }
    }
    pub(crate) fn ensure_open(&self) -> Result<()> {
        self.router.ensure_open()
    }
    pub(crate) fn close(&self) {
        self.router.close();
    }
    pub(crate) fn unregister_deployment(&self, deployment: DeploymentId) {
        self.router.unregister_deployment(deployment);
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
            Context::current().filter(|ctx| Arc::ptr_eq(&ctx.inner.event_bus.router, &self.router))
        });
        self.router.register(address, owner, options.capacity())
    }
}
