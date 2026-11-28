use super::route::{RouteKind, Router, SubscriptionOwner};
use super::{Consumer, ConsumerOptions};
use crate::Result;
use crate::context::Context;
use crate::diagnostics::Diagnostics;
use crate::scope::{ResourceOwner, ResourceRegistration, WorkScope};
use std::sync::Arc;

/// Local typed address router. Consumers registered within managed execution
/// of this instance inherit that scope. Send awaits capacity; publish drops on
/// full queues.
#[derive(Clone)]
pub struct EventBus {
    router: Arc<Router>,
}
impl EventBus {
    pub(crate) fn new(scope: &WorkScope, diagnostics: Arc<Diagnostics>) -> Self {
        Self {
            router: Arc::new(Router::new(scope, diagnostics)),
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
    /// body type or a typed request protocol returns `TypeMismatch`.
    /// Registration uses this instance's current managed scope when present. An unbound consumer supports pull receiving.
    /// Dropping the consumer unregisters it.
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
        self.consumer_for(address, options, RouteKind::message::<T>())
    }

    pub(super) fn consumer_for<T: Send + 'static>(
        &self,
        address: &str,
        options: ConsumerOptions,
        kind: RouteKind,
    ) -> Result<Consumer<T>> {
        options.validate()?;
        self.ensure_open()?;
        let context = Context::current()
            .filter(|ctx| Arc::ptr_eq(&ctx.instance_event_bus().router, &self.router));
        let cancel = context
            .as_ref()
            .map(|ctx| ctx.scope().cancel.clone())
            .unwrap_or_else(|| self.router.closed.clone())
            .child_token();
        let metadata = context.as_ref().map(|ctx| SubscriptionOwner {
            context_id: ctx.id(),
            deployment_id: ctx.deployment_id(),
        });
        let register = || {
            self.router
                .register(address, kind, metadata, cancel, options.capacity())
        };
        let (registration, owner) = match context {
            Some(ctx) => {
                let scope = ctx.scope();
                let registration = scope.with_admission(|mut admission| {
                    let registration = register()?;
                    admission.register_resource(
                        registration.subscription.id,
                        ResourceRegistration::Subscription(registration.subscription.clone()),
                    );
                    Ok(registration)
                })?;
                (registration, Some(ResourceOwner::new(ctx.id(), scope)))
            }
            None => (register()?, None),
        };
        let consumer = Consumer::new(registration, owner);
        // A global close can cancel before it acquires the router's admission lock.
        self.ensure_open()?;
        Ok(consumer)
    }
}
