use super::{Diagnostics, DiagnosticsOptions, DiagnosticsSnapshot, QueueSnapshot, State};
use crate::{ContextId, DeploymentId};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

/// Small metadata only: never owns a sender, receiver, message or Context.
pub(crate) struct QueueStats {
    metadata: QueueSnapshot,
    pub queued: AtomicUsize,
    pub reserved: AtomicUsize,
    pub waiting: AtomicUsize,
    full: AtomicU64,
    diagnostics: Weak<Diagnostics>,
}

pub(super) struct QueueSample {
    stats: Arc<QueueStats>,
    queued: usize,
    reserved: usize,
    waiting: usize,
    full: u64,
}

impl QueueSample {
    pub(super) fn snapshot(self) -> QueueSnapshot {
        let mut out = self.stats.metadata.clone();
        out.queued = self.queued;
        out.reserved = self.reserved;
        out.waiting_senders = self.waiting;
        out.full_events = self.full;
        out
    }
}

impl QueueStats {
    pub fn full(&self) {
        self.full.fetch_add(1, Ordering::Relaxed);
        if let Some(diagnostics) = self.diagnostics.upgrade() {
            diagnostics.state.lock().unwrap().summary.queue_full_events += 1;
        }
    }
}
impl Diagnostics {
    pub fn register_queue(
        self: &Arc<Self>,
        consumer_id: u64,
        address: &str,
        capacity: usize,
        context_id: Option<ContextId>,
        deployment_id: Option<DeploymentId>,
    ) -> Option<Arc<QueueStats>> {
        if !self.options.enabled {
            return None;
        }
        let stats = Arc::new(QueueStats {
            metadata: QueueSnapshot {
                consumer_id,
                address: address.into(),
                capacity,
                context_id,
                deployment_id,
                queued: 0,
                reserved: 0,
                waiting_senders: 0,
                full_events: 0,
            },
            queued: AtomicUsize::new(0),
            reserved: AtomicUsize::new(0),
            waiting: AtomicUsize::new(0),
            full: AtomicU64::new(0),
            diagnostics: Arc::downgrade(self),
        });
        self.state
            .lock()
            .unwrap()
            .queues
            .insert(consumer_id, Arc::downgrade(&stats));
        Some(stats)
    }
    pub fn unregister_queue(&self, id: u64) {
        if self.options.enabled {
            self.state.lock().unwrap().queues.remove(&id);
        }
    }
}

impl State {
    pub(super) fn sample_queues(
        &self,
        out: &mut DiagnosticsSnapshot,
        options: &DiagnosticsOptions,
    ) -> Vec<QueueSample> {
        let mut samples = Vec::new();
        for stats in self.queues.values().filter_map(Weak::upgrade) {
            let queued = stats.queued.load(Ordering::Relaxed);
            let reserved = stats.reserved.load(Ordering::Relaxed);
            let waiting = stats.waiting.load(Ordering::Relaxed);
            out.queued_messages += queued;
            out.reserved_slots += reserved;
            out.waiting_senders += waiting;
            if options.queue_details && samples.len() < options.max_queue_details {
                let full = stats.full.load(Ordering::Relaxed);
                samples.push(QueueSample {
                    stats,
                    queued,
                    reserved,
                    waiting,
                    full,
                });
            }
        }
        out.queue_details_truncated = options.queue_details && self.queues.len() > samples.len();
        samples
    }
}
