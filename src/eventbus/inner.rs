use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};

use super::consumer::Consumer;
use super::envelope::ErasedEnvelope;
use crate::{Result, VertxError};

const CHANNEL_CAPACITY: usize = 256;

pub(crate) type SubscriberId = u64;

pub(crate) struct SubscriberHandle {
    pub id: SubscriberId,
    pub tx: mpsc::Sender<ErasedEnvelope>,
    pub type_id: TypeId,
}

pub(crate) struct EventBusInner {
    routes: DashMap<String, Vec<SubscriberHandle>>,
    rr: DashMap<String, AtomicUsize>,
    next_id: AtomicU64,
}

impl EventBusInner {
    pub fn new() -> Self {
        Self {
            routes: DashMap::new(),
            rr: DashMap::new(),
            next_id: AtomicU64::new(1),
        }
    }

    pub fn register<T: Send + 'static>(self: &Arc<Self>, address: &str) -> Consumer<T> {
        let (tx, rx) = mpsc::channel::<ErasedEnvelope>(CHANNEL_CAPACITY);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let handle = SubscriberHandle { id, tx, type_id: TypeId::of::<T>() };
        self.routes.entry(address.to_string()).or_default().push(handle);
        Consumer::new(rx, address.to_string(), id, Arc::clone(self))
    }

    pub fn unregister(&self, address: &str, id: SubscriberId) {
        if let Some(mut entry) = self.routes.get_mut(address) {
            entry.retain(|h| h.id != id);
            if entry.is_empty() {
                drop(entry);
                self.routes.remove(address);
            }
        }
    }

    /// Broadcast a clonable message to every subscriber on `address`.
    /// Delivery to each subscriber is best-effort (drop-on-full).
    pub fn publish<T: Clone + Send + 'static>(
        &self,
        address: &str,
        body: T,
        headers: HashMap<String, String>,
    ) -> Result<()> {
        let Some(subs) = self.routes.get(address) else {
            return Ok(()); // publish to non-existent address is silent
        };
        let type_id = TypeId::of::<T>();
        // Snapshot senders so we don't hold the dashmap guard while sending.
        let senders: Vec<mpsc::Sender<ErasedEnvelope>> = subs
            .iter()
            .filter(|h| h.type_id == type_id)
            .map(|h| h.tx.clone())
            .collect();
        let skipped = subs.len() - senders.len();
        drop(subs);
        if skipped > 0 {
            return Err(VertxError::TypeMismatch(address.to_string()));
        }
        for tx in senders {
            let env = ErasedEnvelope {
                body: Box::new(body.clone()),
                headers: headers.clone(),
                reply_to: None,
            };
            let _ = tx.try_send(env); // drop-on-full per design
        }
        Ok(())
    }

    /// Async point-to-point. Awaits if the chosen subscriber's queue is full
    /// (cooperative backpressure).
    pub async fn send<T: Send + 'static>(
        &self,
        address: &str,
        body: T,
        headers: HashMap<String, String>,
    ) -> Result<()> {
        let (sender, env) = self.prepare_send::<T>(address, body, headers, None)?;
        sender
            .send(env)
            .await
            .map_err(|_| VertxError::Other(format!("channel closed: {address}")))
    }

    /// Non-blocking point-to-point. Returns an error if the chosen subscriber's
    /// queue is full or closed.
    pub fn try_send<T: Send + 'static>(
        &self,
        address: &str,
        body: T,
        headers: HashMap<String, String>,
    ) -> Result<()> {
        let (sender, env) = self.prepare_send::<T>(address, body, headers, None)?;
        sender
            .try_send(env)
            .map_err(|_| VertxError::Other(format!("channel full or closed: {address}")))
    }

    pub async fn send_with_reply<T: Send + 'static>(
        &self,
        address: &str,
        body: T,
        headers: HashMap<String, String>,
    ) -> Result<oneshot::Receiver<Box<dyn Any + Send>>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let (sender, env) = self.prepare_send::<T>(address, body, headers, Some(reply_tx))?;
        sender
            .send(env)
            .await
            .map_err(|_| VertxError::Other(format!("channel closed: {address}")))?;
        Ok(reply_rx)
    }

    fn prepare_send<T: Send + 'static>(
        &self,
        address: &str,
        body: T,
        headers: HashMap<String, String>,
        reply_to: Option<oneshot::Sender<Box<dyn Any + Send>>>,
    ) -> Result<(mpsc::Sender<ErasedEnvelope>, ErasedEnvelope)> {
        let Some(subs) = self.routes.get(address) else {
            return Err(VertxError::NoConsumer(address.to_string()));
        };
        if subs.is_empty() {
            return Err(VertxError::NoConsumer(address.to_string()));
        }
        let cursor = self
            .rr
            .entry(address.to_string())
            .or_insert_with(|| AtomicUsize::new(0));
        let idx = cursor.fetch_add(1, Ordering::Relaxed) % subs.len();
        let h = &subs[idx];
        if h.type_id != TypeId::of::<T>() {
            return Err(VertxError::TypeMismatch(address.to_string()));
        }
        let tx = h.tx.clone();
        drop(subs);
        let env = ErasedEnvelope {
            body: Box::new(body),
            headers,
            reply_to,
        };
        Ok((tx, env))
    }
}
