use super::Headers;
use super::consumer::Consumer;
use super::envelope::{ErasedEnvelope, MessageTrace};
use crate::{Context, DeploymentId, Result, VertxError};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use std::any::TypeId;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

pub(crate) type SubscriberId = u64;
/// Shared subscription state; the consumer deliberately does not own a sender.
pub(crate) struct QueueState {
    deployment: Option<DeploymentId>,
    pub cancel: CancellationToken,
    active: std::sync::atomic::AtomicBool,
    pub stats: Option<Arc<crate::diagnostics::QueueStats>>,
}
impl QueueState {
    fn mark_inactive(&self) {
        self.active.store(false, Ordering::Release);
    }
    pub fn dequeued(&self) {
        if let Some(stats) = &self.stats {
            stats.queued.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
#[derive(Clone)]
pub(crate) struct Target {
    id: SubscriberId,
    tx: mpsc::Sender<ErasedEnvelope>,
    pub state: Arc<QueueState>,
    address: String,
    closed: CancellationToken,
}
impl Target {
    fn check(&self) -> Result<()> {
        if self.closed.is_cancelled() {
            Err(VertxError::VertxClosed)
        } else if !self.state.active.load(Ordering::Acquire)
            || self.state.cancel.is_cancelled()
            || self.tx.is_closed()
        {
            Err(VertxError::ChannelClosed(self.address.clone()))
        } else {
            Ok(())
        }
    }
    pub fn reserve(self) -> Reservation {
        let acquire = Box::pin(self.tx.clone().reserve_owned());
        let cancel = Box::pin(self.state.cancel.clone().cancelled_owned());
        Reservation {
            target: self,
            acquire: Some(acquire),
            cancel,
            permit: None,
            waiting: false,
        }
    }
    fn try_reserve(self) -> Result<Reservation> {
        self.check()?;
        let permit = self
            .tx
            .clone()
            .try_reserve_owned()
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    if let Some(stats) = &self.state.stats {
                        stats.full();
                    }
                    VertxError::QueueFull(self.address.clone())
                }
                mpsc::error::TrySendError::Closed(_) => {
                    VertxError::ChannelClosed(self.address.clone())
                }
            })?;
        if let Some(stats) = &self.state.stats {
            stats.reserved.fetch_add(1, Ordering::Relaxed);
        }
        let cancel = Box::pin(self.state.cancel.clone().cancelled_owned());
        Ok(Reservation {
            target: self,
            acquire: None,
            cancel,
            permit: Some(permit),
            waiting: false,
        })
    }
}
type Acquire = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = std::result::Result<
                    mpsc::OwnedPermit<ErasedEnvelope>,
                    mpsc::error::SendError<()>,
                >,
            > + Send,
    >,
>;
/// One queue position and, once ready, one slot. Drop releases both accounting
/// and Tokio's reservation even if a waiting sender is aborted before wakeup.
pub(crate) struct Reservation {
    target: Target,
    acquire: Option<Acquire>,
    cancel: std::pin::Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>,
    permit: Option<mpsc::OwnedPermit<ErasedEnvelope>>,
    waiting: bool,
}
impl Reservation {
    pub fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<()>> {
        use std::future::Future;
        use std::task::Poll;
        if let Err(error) = self.target.check() {
            return Poll::Ready(Err(error));
        }
        if self.cancel.as_mut().poll(cx).is_ready() {
            return Poll::Ready(self.target.check());
        }
        if self.permit.is_some() {
            return Poll::Ready(Ok(()));
        }
        match self
            .acquire
            .as_mut()
            .expect("unreserved sender owns an acquire future")
            .as_mut()
            .poll(cx)
        {
            Poll::Pending => {
                if !self.waiting {
                    self.waiting = true;
                    if let Some(stats) = &self.target.state.stats {
                        stats.waiting.fetch_add(1, Ordering::Relaxed);
                        // Tokio may also yield for its cooperative budget;
                        // Pending alone does not prove the queue is full.
                        if self.target.tx.capacity() == 0 {
                            stats.full();
                        }
                    }
                }
                Poll::Pending
            }
            Poll::Ready(result) => {
                self.end_wait();
                self.acquire = None;
                match result {
                    Ok(permit) => {
                        self.permit = Some(permit);
                        if let Some(stats) = &self.target.state.stats {
                            stats.reserved.fetch_add(1, Ordering::Relaxed);
                        }
                        Poll::Ready(self.target.check())
                    }
                    Err(_) => {
                        Poll::Ready(Err(VertxError::ChannelClosed(self.target.address.clone())))
                    }
                }
            }
        }
    }
    fn end_wait(&mut self) {
        if self.waiting {
            self.waiting = false;
            if let Some(stats) = &self.target.state.stats {
                stats.waiting.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
    pub fn send(mut self, mut envelope: ErasedEnvelope) -> Result<()> {
        // Tracing callbacks run outside router locks and before the final check.
        let trace = envelope.trace.mark_enqueued();
        self.target.check()?;
        let permit = self
            .permit
            .take()
            .ok_or_else(|| VertxError::Other("sink is not ready".into()))?;
        if let Some(stats) = &self.target.state.stats {
            stats.reserved.fetch_sub(1, Ordering::Relaxed);
            // Increment before waking a receiver which may immediately dequeue.
            stats.queued.fetch_add(1, Ordering::Relaxed);
        }
        permit.send(envelope);
        if let Some(trace) = trace {
            trace.log_enqueued();
        }
        Ok(())
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.end_wait();
        if self.permit.is_some()
            && let Some(stats) = &self.target.state.stats
        {
            stats.reserved.fetch_sub(1, Ordering::Relaxed);
        }
    }
}
struct Route {
    kind: TypeId,
    subscribers: Vec<Target>,
    cursor: usize,
}
pub(crate) struct EventBusInner {
    pub diagnostics: Arc<crate::diagnostics::Diagnostics>,
    routes: DashMap<String, Route>,
    admission: RwLock<()>,
    next_id: AtomicU64,
    pub closed: CancellationToken,
}
impl EventBusInner {
    pub fn new(
        closed: CancellationToken,
        diagnostics: Arc<crate::diagnostics::Diagnostics>,
    ) -> Self {
        Self {
            diagnostics,
            routes: DashMap::new(),
            admission: RwLock::new(()),
            next_id: AtomicU64::new(1),
            closed,
        }
    }
    pub fn ensure_open(&self) -> Result<()> {
        if self.closed.is_cancelled() {
            Err(VertxError::VertxClosed)
        } else {
            Ok(())
        }
    }
    pub fn register<T: Send + 'static>(
        self: &Arc<Self>,
        address: &str,
        owner: Option<Context>,
        capacity: usize,
    ) -> Result<Consumer<T>> {
        // A deployment cannot start stopping until its route has been installed.
        let scope_admission = match &owner {
            Some(ctx) => Some(ctx.scope.admit()?),
            None => None,
        };
        let bus_admission = self.admission.read().unwrap();
        self.ensure_open()?;
        let (tx, rx) = mpsc::channel(capacity);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let deployment = owner.as_ref().and_then(Context::deployment_id);
        let cancel = owner
            .as_ref()
            .map(|c| c.cancellation_token())
            .unwrap_or_else(|| self.closed.clone())
            .child_token();
        let stats = self.diagnostics.register_queue(
            id,
            address,
            capacity,
            owner.as_ref().map(Context::id),
            deployment,
        );
        let state = Arc::new(QueueState {
            deployment,
            cancel: cancel.clone(),
            active: std::sync::atomic::AtomicBool::new(true),
            stats,
        });
        {
            let mut route = self.routes.entry(address.into()).or_insert_with(|| Route {
                kind: TypeId::of::<T>(),
                subscribers: Vec::new(),
                cursor: 0,
            });
            if route.kind != TypeId::of::<T>() {
                self.diagnostics.unregister_queue(id);
                return Err(VertxError::TypeMismatch(address.into()));
            }
            route.subscribers.push(Target {
                id,
                tx,
                state: state.clone(),
                address: address.into(),
                closed: self.closed.clone(),
            });
            self.diagnostics.consumer(1);
        }
        // Rollback can cancel the target or drop a receiver that wakes senders.
        drop(bus_admission);
        drop(scope_admission);
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
    pub fn unregister(&self, address: &str, id: SubscriberId) {
        let removed = if let Entry::Occupied(mut entry) = self.routes.entry(address.into()) {
            if let Some(index) = entry.get().subscribers.iter().position(|t| t.id == id) {
                let target = entry.get_mut().subscribers.remove(index);
                self.mark_removed(&target);
                self.diagnostics.consumer(-1);
                if entry.get().subscribers.is_empty() {
                    entry.remove();
                }
                Some(target)
            } else {
                None
            }
        } else {
            None
        };
        // Cancellation and the last sender's Drop can invoke user Wakers.
        // Both must run after releasing the route entry guard.
        if let Some(target) = removed {
            target.state.cancel.cancel();
        }
    }
    pub fn close(&self) {
        self.closed.cancel();
        let admission = self.admission.write().unwrap();
        let removed = self.remove_matching(|_| true);
        drop(admission);
        for target in removed {
            target.state.cancel.cancel();
        }
    }
    pub fn unregister_deployment(&self, deployment: DeploymentId) {
        for target in self.remove_matching(|target| target.state.deployment == Some(deployment)) {
            target.state.cancel.cancel();
        }
    }
    fn remove_matching(&self, mut matches: impl FnMut(&Target) -> bool) -> Vec<Target> {
        let mut removed = Vec::new();
        self.routes.retain(|_, route| {
            let count = route.subscribers.len();
            for target in route.subscribers.extract_if(.., |target| matches(target)) {
                self.mark_removed(&target);
                removed.push(target);
            }
            let removed_count = count - route.subscribers.len();
            if removed_count > 0 {
                self.diagnostics.consumer(-(removed_count as isize));
            }
            !route.subscribers.is_empty()
        });
        removed
    }
    fn mark_removed(&self, target: &Target) {
        target.state.mark_inactive();
        self.diagnostics.unregister_queue(target.id);
    }
    pub fn publish<T: Clone + Send + 'static>(
        &self,
        address: &str,
        body: T,
        headers: Headers,
    ) -> Result<()> {
        self.ensure_open()?;
        let senders: Vec<_> = match self.routes.get(address) {
            None => return Ok(()),
            Some(route) => {
                if route.kind != TypeId::of::<T>() {
                    return Err(VertxError::TypeMismatch(address.into()));
                }
                route.subscribers.clone()
            }
        };
        let mut dropped = 0usize;
        for target in senders {
            match target.try_reserve() {
                Ok(reservation) => {
                    // Clone is user code; only invoke it after admission, outside route locks.
                    let envelope = ErasedEnvelope {
                        trace: MessageTrace::new(&self.diagnostics, address, None),
                        body: Box::new(body.clone()),
                        headers: headers.clone(),
                        reply_to: None,
                    };
                    if reservation.send(envelope).is_err() {
                        dropped += 1;
                    }
                }
                Err(_) => dropped += 1,
            }
        }
        self.diagnostics.dropped(dropped);
        if dropped > 0 && self.diagnostics.options.enabled {
            tracing::warn!(
                instance_id = self.diagnostics.instance,
                address,
                dropped,
                "broadcast delivery dropped"
            );
        }
        Ok(())
    }
    pub fn select<T: Send + 'static>(&self, address: &str) -> Result<Target> {
        self.ensure_open()?;
        let mut route = self
            .routes
            .get_mut(address)
            .ok_or_else(|| VertxError::NoConsumer(address.into()))?;
        if route.kind != TypeId::of::<T>() {
            return Err(VertxError::TypeMismatch(address.into()));
        }
        let index = route.cursor % route.subscribers.len();
        route.cursor = route.cursor.wrapping_add(1);
        Ok(route.subscribers[index].clone())
    }
    fn prepare<T: Send + 'static>(
        &self,
        address: &str,
        body: T,
        headers: Headers,
        reply_to: Option<oneshot::Sender<Result<ErasedEnvelope>>>,
        request_id: Option<u64>,
    ) -> Result<(Target, ErasedEnvelope)> {
        let target = self.select::<T>(address)?;
        Ok((
            target,
            ErasedEnvelope {
                trace: MessageTrace::new(&self.diagnostics, address, request_id),
                body: Box::new(body),
                headers,
                reply_to,
            },
        ))
    }
    async fn deliver(&self, target: Target, envelope: ErasedEnvelope) -> Result<()> {
        let mut reservation = target.reserve();
        futures::future::poll_fn(|cx| reservation.poll_ready(cx)).await?;
        reservation.send(envelope)
    }
    pub async fn send<T: Send + 'static>(
        &self,
        address: &str,
        body: T,
        headers: Headers,
    ) -> Result<()> {
        let (tx, env) = self.prepare(address, body, headers, None, None)?;
        self.deliver(tx, env).await
    }
    pub fn try_send<T: Send + 'static>(
        &self,
        address: &str,
        body: T,
        headers: Headers,
    ) -> Result<()> {
        let (tx, env) = self.prepare(address, body, headers, None, None)?;
        tx.try_reserve()?.send(env)
    }
    pub async fn send_with_reply<T: Send + 'static>(
        &self,
        request_id: u64,
        address: &str,
        body: T,
        headers: Headers,
    ) -> Result<oneshot::Receiver<Result<ErasedEnvelope>>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let (tx, env) = self.prepare(address, body, headers, Some(reply_tx), Some(request_id))?;
        self.deliver(tx, env).await?;
        Ok(reply_rx)
    }
}

#[cfg(test)]
mod tests {
    //! Route lifecycle, reentrant wakeups and admission accounting.
    use super::*;

    #[rstest::fixture]
    fn bus() -> Arc<EventBusInner> {
        Arc::new(EventBusInner::new(
            CancellationToken::new(),
            crate::diagnostics::Diagnostics::new(0, Default::default()),
        ))
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn malformed_envelope_reports_conversion_failure_once(bus: Arc<EventBusInner>) {
        let mut consumer = bus.register::<u32>("typed", None, 1).unwrap();
        bus.select::<u32>("typed")
            .unwrap()
            .try_reserve()
            .unwrap()
            .send(ErasedEnvelope {
                trace: MessageTrace::new(&bus.diagnostics, "typed", None),
                body: Box::new("wrong type"),
                headers: Headers::new(),
                reply_to: None,
            })
            .unwrap();
        assert!(matches!(
            consumer.recv().await,
            Err(VertxError::TypeMismatch(_))
        ));
        assert!(consumer.recv().await.unwrap().is_none());
        assert!(bus.routes.is_empty());
    }
    #[rstest::rstest]
    #[tokio::test]
    async fn concurrent_last_unregister_and_new_register_preserve_new_route(
        bus: Arc<EventBusInner>,
    ) {
        for _ in 0..100 {
            let old = bus.register::<u32>("race", None, 256).unwrap();
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let b = barrier.clone();
            let dropper = std::thread::spawn(move || {
                b.wait();
                drop(old);
            });
            let b = barrier.clone();
            let router = bus.clone();
            let creator = std::thread::spawn(move || {
                b.wait();
                router.register::<u32>("race", None, 256).unwrap()
            });
            dropper.join().unwrap();
            let mut new = creator.join().unwrap();
            bus.try_send("race", 42u32, Headers::new()).unwrap();
            assert_eq!(new.recv().await.unwrap().unwrap().into_body(), 42);
        }
        assert!(bus.routes.is_empty());
    }

    #[rstest::rstest]
    #[test]
    fn unregister_wakes_waiters_after_releasing_the_route_lock(bus: Arc<EventBusInner>) {
        struct Reenter {
            bus: Arc<EventBusInner>,
            wakes: std::sync::atomic::AtomicUsize,
        }
        impl std::task::Wake for Reenter {
            fn wake(self: Arc<Self>) {
                assert!(!matches!(
                    self.bus.routes.try_get("wake"),
                    dashmap::try_result::TryResult::Locked
                ));
                self.wakes.fetch_add(1, Ordering::SeqCst);
            }
        }
        let consumer = bus.register::<u32>("wake", None, 1).unwrap();
        bus.try_send("wake", 1u32, Headers::new()).unwrap();
        let mut reservation = bus.select::<u32>("wake").unwrap().reserve();
        let reenter = Arc::new(Reenter {
            bus: bus.clone(),
            wakes: std::sync::atomic::AtomicUsize::new(0),
        });
        let waker = std::task::Waker::from(reenter.clone());
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(reservation.poll_ready(&mut cx).is_pending());
        drop(consumer);
        assert!(reenter.wakes.load(Ordering::SeqCst) > 0);
        assert!(matches!(
            reservation.poll_ready(&mut cx),
            std::task::Poll::Ready(Err(VertxError::ChannelClosed(_)))
        ));
    }

    #[test]
    fn concurrent_close_and_unregister_leave_no_queue_counts() {
        for _ in 0..100 {
            let bus = bus();
            let consumer = bus.register::<u32>("close-race", None, 1).unwrap();
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let gate = barrier.clone();
            let dropper = std::thread::spawn(move || {
                gate.wait();
                drop(consumer);
            });
            barrier.wait();
            bus.close();
            dropper.join().unwrap();
            let snapshot = bus.diagnostics.snapshot();
            assert_eq!(snapshot.active_consumers, 0);
            assert!(snapshot.queues.is_empty());
        }
    }
}
