//! Read-only sampling diagnostics. No executor locks, user futures or Contexts
//! are retained here; snapshots are not globally transactional.

mod queue;
mod snapshot;
mod task;

pub(crate) use queue::QueueStats;
pub use snapshot::{
    DeploymentSnapshot, DiagnosticsSnapshot, QueueSnapshot, TaskSnapshot, ThreadSnapshot,
};
pub(crate) use task::{OperationGuard, TaskRecord};

use crate::{DeploymentId, TaskId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeploymentStage {
    Queued,
    Starting,
    Running,
    Quiescing,
    Stopping,
    Stopped,
}
impl DeploymentStage {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Quiescing => "quiescing",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum CloseStage {
    #[default]
    Running,
    Disabled,
    Deployments,
    Blocking,
    WorkerJoin,
    EventLoopJoin,
    Closed,
    Failed,
}
impl CloseStage {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Disabled => "disabled",
            Self::Deployments => "deployments",
            Self::Blocking => "blocking",
            Self::WorkerJoin => "worker_join",
            Self::EventLoopJoin => "event_loop_join",
            Self::Closed => "closed",
            Self::Failed => "failed",
        }
    }
}

/// Lightweight counters and current synchronous operations are enabled by default.
/// Task and queue details are opt-in with bounded snapshot output; no history
/// is retained. Queue summary metadata scales with registered consumers.
#[derive(Debug, Clone)]
pub struct DiagnosticsOptions {
    /// Enables runtime counters, current operations and optional detail collection.
    pub enabled: bool,
    /// Includes individual live task records in a snapshot.
    pub task_details: bool,
    /// Includes individual registered queue records in a snapshot.
    pub queue_details: bool,
    /// Caps the number of queue details returned per snapshot.
    pub max_queue_details: usize,
    /// Caps the number of task details tracked concurrently.
    pub max_task_details: usize,
    /// Logs a slow synchronous poll or drop on an EventLoop after it returns.
    pub event_loop_poll_threshold: Duration,
    /// Logs a slow synchronous poll or drop on a Worker after it returns.
    pub worker_poll_threshold: Duration,
    /// Logs a long blocking closure after it returns.
    pub blocking_threshold: Duration,
}
impl Default for DiagnosticsOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            task_details: false,
            queue_details: false,
            max_queue_details: 1024,
            max_task_details: 1024,
            event_loop_poll_threshold: Duration::from_millis(100),
            worker_poll_threshold: Duration::from_secs(1),
            blocking_threshold: Duration::from_secs(1),
        }
    }
}
/// Current scheduling phase of a managed task or blocking operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStage {
    /// Submitted or woken and awaiting its next poll.
    Ready,
    /// Returned `Pending` and awaits a wake.
    Waiting,
    /// Executing a synchronous future poll.
    Polling,
    /// Awaiting the source Context's FIFO blocking admission.
    BlockingOrderedWait,
    /// Submitted to the shared Worker pool.
    BlockingQueued,
    /// Executing on a Worker thread.
    BlockingRunning,
}
#[derive(Default)]
struct State {
    summary: DiagnosticsSnapshot,
    tasks: HashMap<TaskId, (TaskSnapshot, Instant)>,
    queues: HashMap<u64, Weak<QueueStats>>,
    threads: HashMap<std::thread::ThreadId, ThreadSnapshot>,
    deployments: HashMap<DeploymentId, DeploymentStage>,
    close_stage: CloseStage,
}
pub(crate) struct Diagnostics {
    pub instance: u64,
    pub options: DiagnosticsOptions,
    state: Mutex<State>,
}
impl Diagnostics {
    pub fn new(instance: u64, options: DiagnosticsOptions) -> Arc<Self> {
        Arc::new(Self {
            instance,
            options,
            state: Mutex::new(State::default()),
        })
    }
    pub fn deployment(&self, id: DeploymentId, stage: DeploymentStage) {
        self.record_deployment(id, stage);
        self.trace_deployment(id, stage);
    }

    pub fn record_deployment(&self, id: DeploymentId, stage: DeploymentStage) {
        if !self.options.enabled {
            return;
        }
        let mut state = self.state.lock().unwrap();
        if stage == DeploymentStage::Stopped {
            state.deployments.remove(&id);
        } else {
            state.deployments.insert(id, stage);
        }
    }

    pub fn trace_deployment(&self, id: DeploymentId, stage: DeploymentStage) {
        if self.options.enabled {
            tracing::debug!(instance_id = self.instance, deployment_id = %id, stage = stage.as_str(), "deployment stage");
        }
    }
    pub fn close_stage(&self, stage: CloseStage) {
        if !self.options.enabled {
            return;
        }
        self.state.lock().unwrap().close_stage = stage;
        tracing::debug!(
            instance_id = self.instance,
            stage = stage.as_str(),
            "close stage"
        );
    }
    pub fn consumer(&self, delta: isize) {
        if self.options.enabled {
            let mut s = self.state.lock().unwrap();
            s.summary.active_consumers = s.summary.active_consumers.saturating_add_signed(delta);
        }
    }
    pub fn timeout(&self) {
        if self.options.enabled {
            self.state.lock().unwrap().summary.request_timeouts += 1;
        }
    }
    pub fn dropped(&self, n: usize) {
        if self.options.enabled {
            self.state.lock().unwrap().summary.broadcast_dropped += n as u64;
        }
    }
}
