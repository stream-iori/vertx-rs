use super::consumer::Consumer;
use super::envelope::ErasedEnvelope;
use super::queue::{QueueSender, QueueState};
use crate::diagnostics::Diagnostics;
use crate::{Context, DeploymentId, Result, VertxError};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use std::any::TypeId;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub(super) type SubscriberId = u64;

struct Subscription {
    id: SubscriberId,
    deployment: Option<DeploymentId>,
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

struct Registration {
    id: SubscriberId,
    rx: mpsc::Receiver<ErasedEnvelope>,
    state: Arc<QueueState>,
}

pub(super) struct Router {
    pub(super) diagnostics: Arc<Diagnostics>,
    routes: DashMap<String, Route>,
    admission: RwLock<()>,
    next_id: AtomicU64,
    pub(super) closed: CancellationToken,
}

impl Router {
    pub(super) fn new(closed: CancellationToken, diagnostics: Arc<Diagnostics>) -> Self {
        Self {
            diagnostics,
            routes: DashMap::new(),
            admission: RwLock::new(()),
            next_id: AtomicU64::new(1),
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

    pub(super) fn register<T: Send + 'static>(
        self: &Arc<Self>,
        address: &str,
        owner: Option<Context>,
        capacity: usize,
    ) -> Result<Consumer<T>> {
        let Registration { id, rx, state } = {
            // Hold both admissions until the subscription is visible in the route.
            let _scope_admission = owner.as_ref().map(|ctx| ctx.scope.admit()).transpose()?;
            let _bus_admission = self.admission.read().unwrap();
            self.ensure_open()?;
            self.insert::<T>(address, owner.as_ref(), capacity)?
        };
        // Closing can cancel the bus token before it acquires the admission lock.
        if self.closed.is_cancelled() {
            self.unregister(address, id);
            return Err(VertxError::VertxClosed);
        }
        Ok(Consumer::new(
            rx,
            address.into(),
            id,
            self.clone(),
            owner,
            state,
        ))
    }

    fn insert<T: 'static>(
        &self,
        address: &str,
        owner: Option<&Context>,
        capacity: usize,
    ) -> Result<Registration> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut route = self
            .routes
            .entry(address.into())
            .or_insert_with(Route::new::<T>);
        route.check_type::<T>(address)?;
        let deployment = owner.and_then(Context::deployment_id);
        let cancel = owner
            .map(Context::cancellation_token)
            .unwrap_or_else(|| self.closed.clone())
            .child_token();
        let stats = self.diagnostics.register_queue(
            id,
            address,
            capacity,
            owner.map(Context::id),
            deployment,
        );
        let (sender, rx) =
            QueueSender::channel(address, capacity, self.closed.clone(), cancel, stats);
        let state = sender.state.clone();
        route.subscribers.push(Subscription {
            id,
            deployment,
            sender,
        });
        self.diagnostics.consumer(1);
        Ok(Registration { id, rx, state })
    }

    pub(super) fn unregister(&self, address: &str, id: SubscriberId) {
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
            self.remove_matching(|_| true)
        };
        for sender in removed {
            sender.cancel();
        }
    }

    pub(super) fn unregister_deployment(&self, deployment: DeploymentId) {
        for sender in self.remove_matching(|sub| sub.deployment == Some(deployment)) {
            sender.cancel();
        }
    }

    fn remove_matching(&self, mut matches: impl FnMut(&Subscription) -> bool) -> Vec<QueueSender> {
        let mut removed = Vec::new();
        self.routes.retain(|_, route| {
            let count = route.subscribers.len();
            for sub in route.subscribers.extract_if(.., |sub| matches(sub)) {
                self.mark_removed(&sub);
                removed.push(sub.sender);
            }
            let removed_count = count - route.subscribers.len();
            if removed_count > 0 {
                self.diagnostics.consumer(-(removed_count as isize));
            }
            !route.subscribers.is_empty()
        });
        removed
    }

    fn mark_removed(&self, sub: &Subscription) {
        sub.sender.mark_inactive();
        self.diagnostics.unregister_queue(sub.id);
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
