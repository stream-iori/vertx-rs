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
    pub enabled: bool,
    pub task_details: bool,
    pub queue_details: bool,
    pub max_queue_details: usize,
    pub max_task_details: usize,
    pub event_loop_poll_threshold: Duration,
    pub worker_poll_threshold: Duration,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStage {
    Ready,
    Waiting,
    Polling,
    BlockingOrderedWait,
    BlockingQueued,
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
        if !self.options.enabled {
            return;
        }
        let mut state = self.state.lock().unwrap();
        if stage == DeploymentStage::Stopped {
            state.deployments.remove(&id);
        } else {
            state.deployments.insert(id, stage);
        }
        drop(state);
        tracing::debug!(instance_id = self.instance, deployment_id = %id, stage = stage.as_str(), "deployment stage");
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
