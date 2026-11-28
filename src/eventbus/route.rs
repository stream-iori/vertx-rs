use super::envelope::ErasedEnvelope;
use super::queue::{QueueSender, QueueState};
use crate::deployment::ResourceId;
use crate::diagnostics::Diagnostics;
use crate::{ContextId, DeploymentId, Result, VertxError};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use std::any::TypeId;
use std::sync::{Arc, RwLock, Weak};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

struct Subscription {
    id: ResourceId,
    sender: QueueSender,
}

struct Route {
    kind: TypeId,
    subscribers: Vec<Subscription>,
    cursor: usize,
}

impl Route {
    fn new<T: 'static>() -> Self {
        Self {
            kind: TypeId::of::<T>(),
            subscribers: Vec::new(),
            cursor: 0,
        }
    }

    fn check_type<T: 'static>(&self, address: &str) -> Result<()> {
        if self.kind == TypeId::of::<T>() {
            Ok(())
        } else {
            Err(VertxError::TypeMismatch(address.into()))
        }
    }
}

pub(super) struct Registration {
    pub subscription: SubscriptionRegistration,
    pub rx: mpsc::Receiver<ErasedEnvelope>,
    pub state: Arc<QueueState>,
}

#[derive(Clone)]
pub(crate) struct SubscriptionRegistration {
    router: Weak<Router>,
    address: Arc<str>,
    pub id: ResourceId,
}

impl SubscriptionRegistration {
    pub(super) fn address(&self) -> &str {
        &self.address
    }

    pub fn unregister(&self) {
        if let Some(router) = self.router.upgrade() {
            router.unregister(&self.address, self.id);
        }
    }
}

pub(super) struct SubscriptionOwner {
    pub context: ContextId,
    pub deployment: DeploymentId,
}

pub(super) struct Router {
    pub(super) diagnostics: Arc<Diagnostics>,
    routes: DashMap<String, Route>,
    admission: RwLock<()>,
    pub(super) closed: CancellationToken,
}

impl Router {
    pub(super) fn new(closed: CancellationToken, diagnostics: Arc<Diagnostics>) -> Self {
        Self {
            diagnostics,
            routes: DashMap::new(),
            admission: RwLock::new(()),
            closed,
        }
    }

    pub(super) fn ensure_open(&self) -> Result<()> {
        if self.closed.is_cancelled() {
            Err(VertxError::VertxClosed)
        } else {
            Ok(())
        }
    }

    pub(super) fn register<T: 'static>(
        self: &Arc<Self>,
        address: &str,
        owner: Option<SubscriptionOwner>,
        cancel: CancellationToken,
        capacity: usize,
    ) -> Result<Registration> {
        let _admission = self.admission.read().unwrap();
        self.ensure_open()?;
        let id = ResourceId::next();
        let mut route = self
            .routes
            .entry(address.into())
            .or_insert_with(Route::new::<T>);
        route.check_type::<T>(address)?;
        let stats = self.diagnostics.register_queue(
            id.0,
            address,
            capacity,
            owner.as_ref().map(|owner| owner.context),
            owner.map(|owner| owner.deployment),
        );
        let (sender, rx) =
            QueueSender::channel(address, capacity, self.closed.clone(), cancel, stats);
        let state = sender.state.clone();
        route.subscribers.push(Subscription { id, sender });
        self.diagnostics.consumer(1);
        Ok(Registration {
            subscription: SubscriptionRegistration {
                router: Arc::downgrade(self),
                address: address.into(),
                id,
            },
            rx,
            state,
        })
    }

    fn unregister(&self, address: &str, id: ResourceId) {
        let sender = {
            let Entry::Occupied(mut entry) = self.routes.entry(address.into()) else {
                return;
            };
            let Some(index) = entry.get().subscribers.iter().position(|sub| sub.id == id) else {
                return;
            };
            let removed = entry.get_mut().subscribers.remove(index);
            self.mark_removed(&removed);
            self.diagnostics.consumer(-1);
            if entry.get().subscribers.is_empty() {
                entry.remove();
            }
            removed.sender
        };
        // Cancellation and sender Drop can wake code that reenters the router.
        sender.cancel();
    }

    pub(super) fn close(&self) {
        self.closed.cancel();
        let removed = {
            let _admission = self.admission.write().unwrap();
            self.remove_all()
        };
        for sender in removed {
            sender.cancel();
        }
    }

    fn remove_all(&self) -> Vec<QueueSender> {
        let mut removed = Vec::new();
        self.routes.retain(|_, route| {
            self.diagnostics
                .consumer(-(route.subscribers.len() as isize));
            for sub in route.subscribers.drain(..) {
                self.mark_removed(&sub);
                removed.push(sub.sender);
            }
            false
        });
        removed
    }

    fn mark_removed(&self, sub: &Subscription) {
        sub.sender.mark_inactive();
        self.diagnostics.unregister_queue(sub.id.0);
    }

    pub(super) fn select<T: 'static>(&self, address: &str) -> Result<QueueSender> {
        self.ensure_open()?;
        let mut route = self
            .routes
            .get_mut(address)
            .ok_or_else(|| VertxError::NoConsumer(address.into()))?;
        route.check_type::<T>(address)?;
        let index = route.cursor % route.subscribers.len();
        route.cursor = route.cursor.wrapping_add(1);
        Ok(route.subscribers[index].sender.clone())
    }

    pub(super) fn subscribers<T: 'static>(&self, address: &str) -> Result<Vec<QueueSender>> {
        self.ensure_open()?;
        let Some(route) = self.routes.get(address) else {
            return Ok(Vec::new());
        };
        route.check_type::<T>(address)?;
        Ok(route
            .subscribers
            .iter()
            .map(|sub| sub.sender.clone())
            .collect())
    }
}

#[cfg(test)]
mod tests;
