use super::consumer::Consumer;
use super::envelope::{ErasedEnvelope, MessageTrace};
use super::Headers;
use crate::{Context, Result, VertxError};
use dashmap::{mapref::entry::Entry, DashMap};
use std::any::TypeId;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

const CHANNEL_CAPACITY: usize = 256;
// Stamp immediately before send; subscriber callbacks run only after the
// envelope is actually queued, so a slow subscriber cannot inflate queue age.
fn enqueue(permit: mpsc::Permit<'_, ErasedEnvelope>, mut envelope: ErasedEnvelope) {
    let trace = envelope.trace.mark_enqueued();
    permit.send(envelope);
    if let Some(trace) = trace {
        trace.log_enqueued();
    }
}
pub(crate) type SubscriberId = u64;
struct Subscriber {
    id: SubscriberId,
    tx: mpsc::Sender<ErasedEnvelope>,
}
struct Route {
    kind: TypeId,
    subscribers: Vec<Subscriber>,
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
    ) -> Result<Consumer<T>> {
        let _admission = self.admission.read().unwrap();
        self.ensure_open()?;
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        {
            let mut route = self.routes.entry(address.into()).or_insert_with(|| Route {
                kind: TypeId::of::<T>(),
                subscribers: Vec::new(),
                cursor: 0,
            });
            if route.kind != TypeId::of::<T>() {
                return Err(VertxError::TypeMismatch(address.into()));
            }
            route.subscribers.push(Subscriber { id, tx });
            self.diagnostics.consumer(1);
        }
        if self.closed.is_cancelled() {
            self.unregister(address, id);
            return Err(VertxError::VertxClosed);
        }
        let cancel = owner
            .as_ref()
            .map(|c| c.cancellation_token())
            .unwrap_or_else(|| self.closed.clone());
        Ok(Consumer::new(
            rx,
            address.into(),
            id,
            self.clone(),
            owner,
            cancel,
        ))
    }
    pub fn unregister(&self, address: &str, id: SubscriberId) {
        if let Entry::Occupied(mut entry) = self.routes.entry(address.into()) {
            let before = entry.get().subscribers.len();
            entry.get_mut().subscribers.retain(|s| s.id != id);
            self.diagnostics
                .consumer(-((before - entry.get().subscribers.len()) as isize));
            if entry.get().subscribers.is_empty() {
                entry.remove();
            }
        }
    }
    pub fn close(&self) {
        self.closed.cancel();
        let _admission = self.admission.write().unwrap();
        let count: usize = self.routes.iter().map(|r| r.subscribers.len()).sum();
        self.routes.clear();
        self.diagnostics.consumer(-(count as isize));
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
                route.subscribers.iter().map(|s| s.tx.clone()).collect()
            }
        };
        let mut dropped = 0usize;
        for tx in senders {
            // Clone outside router locks: Clone is user code and may re-enter the bus.
            let body = Box::new(body.clone());
            match tx.try_reserve() {
                Ok(permit) => {
                    let trace = MessageTrace::new(&self.diagnostics, address, None);
                    enqueue(
                        permit,
                        ErasedEnvelope {
                            trace,
                            body,
                            headers: headers.clone(),
                            reply_to: None,
                        },
                    );
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
    fn prepare<T: Send + 'static>(
        &self,
        address: &str,
        body: T,
        headers: Headers,
        reply_to: Option<oneshot::Sender<Result<ErasedEnvelope>>>,
        request_id: Option<u64>,
    ) -> Result<(mpsc::Sender<ErasedEnvelope>, ErasedEnvelope)> {
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
        let tx = route.subscribers[index].tx.clone();
        drop(route); // Subscriber callbacks must run outside the route lock.
        Ok((
            tx,
            ErasedEnvelope {
                trace: MessageTrace::new(&self.diagnostics, address, request_id),
                body: Box::new(body),
                headers,
                reply_to,
            },
        ))
    }
    async fn deliver(
        &self,
        address: &str,
        tx: mpsc::Sender<ErasedEnvelope>,
        envelope: ErasedEnvelope,
    ) -> Result<()> {
        tokio::select! { biased;
            _ = self.closed.cancelled() => Err(VertxError::VertxClosed),
            r = tx.reserve() => {
                let permit = r.map_err(|_| VertxError::ChannelClosed(address.into()))?;
                enqueue(permit, envelope);
                Ok(())
            },
        }
    }
    pub async fn send<T: Send + 'static>(
        &self,
        address: &str,
        body: T,
        headers: Headers,
    ) -> Result<()> {
        let (tx, env) = self.prepare(address, body, headers, None, None)?;
        self.deliver(address, tx, env).await
    }
    pub fn try_send<T: Send + 'static>(
        &self,
        address: &str,
        body: T,
        headers: Headers,
    ) -> Result<()> {
        let (tx, env) = self.prepare(address, body, headers, None, None)?;
        let permit = tx.try_reserve().map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => VertxError::QueueFull(address.into()),
            mpsc::error::TrySendError::Closed(_) => VertxError::ChannelClosed(address.into()),
        })?;
        enqueue(permit, env);
        Ok(())
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
        self.deliver(address, tx, env).await?;
        Ok(reply_rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn concurrent_last_unregister_and_new_register_preserve_new_route() {
        let bus = Arc::new(EventBusInner::new(
            CancellationToken::new(),
            crate::diagnostics::Diagnostics::new(0, Default::default()),
        ));
        for _ in 0..100 {
            let old = bus.register::<u32>("race", None).unwrap();
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
                router.register::<u32>("race", None).unwrap()
            });
            dropper.join().unwrap();
            let mut new = creator.join().unwrap();
            bus.try_send("race", 42u32, Headers::new()).unwrap();
            assert_eq!(new.recv().await.unwrap().into_body(), 42);
        }
        assert!(bus.routes.is_empty());
    }
}
