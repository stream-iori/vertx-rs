use super::{CloseStage, Diagnostics, DiagnosticsOptions, State, TaskStage};
use crate::{ContextId, DeploymentId, EventLoopId, TaskId};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct TaskSnapshot {
    pub task_id: TaskId,
    pub context_id: ContextId,
    pub deployment_id: DeploymentId,
    pub stage: TaskStage,
    pub elapsed: Duration,
}
#[derive(Debug, Clone)]
pub struct ThreadSnapshot {
    pub thread_id: ThreadId,
    pub event_loop_id: Option<EventLoopId>,
    pub context_id: ContextId,
    pub task_id: TaskId,
    pub operation: &'static str,
    pub started_at: Instant,
}
#[derive(Debug, Clone)]
pub struct DeploymentSnapshot {
    pub deployment_id: DeploymentId,
    pub stage: &'static str,
}
/// Counts only registered consumer queues. Capacity is in messages, not bytes.
#[derive(Debug, Clone)]
pub struct QueueSnapshot {
    pub consumer_id: u64,
    pub address: String,
    pub context_id: Option<ContextId>,
    pub deployment_id: Option<DeploymentId>,
    pub capacity: usize,
    pub queued: usize,
    pub reserved: usize,
    pub waiting_senders: usize,
    pub full_events: u64,
}
/// Sampling view. Counters include lifecycle drivers and blocking work as tasks;
/// threads lists only in-progress managed poll/drop/blocking operations.
/// Naked Tokio tasks and time spent waiting for the Context execution mutex are
/// not monitored. Successful close leaves all active counts at zero.
#[derive(Debug, Clone, Default)]
pub struct DiagnosticsSnapshot {
    pub enabled: bool,
    pub instance_id: u64,
    pub active_deployments: usize,
    pub active_consumers: usize,
    pub queued_messages: usize,
    pub reserved_slots: usize,
    pub waiting_senders: usize,
    pub queue_full_events: u64,
    pub queues: Vec<QueueSnapshot>,
    pub queue_details_truncated: bool,
    pub active_tasks: usize,
    pub ready_tasks: usize,
    pub blocking_waiting: usize,
    pub blocking_running: usize,
    pub completed: u64,
    pub failed: u64,
    pub cancelled: u64,
    pub request_timeouts: u64,
    pub broadcast_dropped: u64,
    pub close_stage: &'static str,
    pub deployments: Vec<DeploymentSnapshot>,
    pub tasks: Vec<TaskSnapshot>,
    pub task_details_truncated: bool,
    pub threads: Vec<ThreadSnapshot>,
}
impl Diagnostics {
    pub fn snapshot(&self) -> DiagnosticsSnapshot {
        let state = self.state.lock().unwrap();
        let mut out = state.summary.clone();
        out.enabled = self.options.enabled;
        out.instance_id = self.instance;
        out.close_stage = if out.enabled {
            state.close_stage
        } else {
            CloseStage::Disabled
        }
        .as_str();
        state.sample_tasks(&mut out, &self.options);
        out.threads = state.threads.values().cloned().collect();
        state.sample_deployments(&mut out);
        state.sample_queues(&mut out, &self.options);
        out
    }
}

impl State {
    fn sample_tasks(&self, out: &mut DiagnosticsSnapshot, options: &DiagnosticsOptions) {
        out.tasks = self
            .tasks
            .values()
            .map(|(task, started)| {
                let mut task = task.clone();
                task.elapsed = started.elapsed();
                task
            })
            .collect();
        out.tasks.sort_by_key(|task| task.task_id);
        out.task_details_truncated = options.task_details && out.active_tasks > out.tasks.len();
    }

    fn sample_deployments(&self, out: &mut DiagnosticsSnapshot) {
        out.deployments = self
            .deployments
            .iter()
            .map(|(&deployment_id, &stage)| DeploymentSnapshot {
                deployment_id,
                stage: stage.as_str(),
            })
            .collect();
        out.active_deployments = out.deployments.len();
    }
}
