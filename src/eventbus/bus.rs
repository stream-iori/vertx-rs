use super::route::{Router, SubscriptionOwner};
use super::{Consumer, ConsumerOptions};
use crate::deployment::ResourceRegistration;
use crate::diagnostics::Diagnostics;
use crate::{Context, Result};
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
    pub(super) fn router(&self) -> &Arc<Router> {
        &self.router
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
            Context::current()
                .filter(|ctx| Arc::ptr_eq(&ctx.instance_event_bus().router, &self.router))
        });
        let cancel = owner
            .as_ref()
            .map(Context::cancellation_token)
            .unwrap_or_else(|| self.router.closed.clone())
            .child_token();
        let registration = {
            let mut admission = owner.as_ref().map(|ctx| ctx.scope().admit()).transpose()?;
            let metadata = owner.as_ref().map(|ctx| SubscriptionOwner {
                context: ctx.id(),
                deployment: ctx.scope().deployment,
            });
            let registration =
                self.router
                    .register::<T>(address, metadata, cancel, options.capacity())?;
            if let Some(admission) = &mut admission {
                admission.register_resource(
                    registration.subscription.id,
                    ResourceRegistration::Subscription(registration.subscription.clone()),
                );
            }
            registration
        };
        let consumer = Consumer::new(registration, owner);
        // A global close can cancel before it acquires the router's admission lock.
        self.ensure_open()?;
        Ok(consumer)
    }
}
