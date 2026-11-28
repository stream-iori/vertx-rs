use super::{CloseStage, Diagnostics, TaskStage};
use crate::{ContextId, DeploymentId, EventLoopId, TaskId};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

/// A sampled record for one live managed operation.
#[derive(Debug, Clone)]
pub struct TaskSnapshot {
    /// Identity shared with the task handle when one is available.
    pub task_id: TaskId,
    /// Logical Context that owns the operation.
    pub context_id: ContextId,
    /// Deployment that tracks completion of the work.
    pub deployment_id: DeploymentId,
    /// Scheduling phase at sampling time.
    pub stage: TaskStage,
    /// Time since submission, including asynchronous waits.
    pub elapsed: Duration,
}
/// One OS thread sampled inside a managed synchronous operation.
#[derive(Debug, Clone)]
pub struct ThreadSnapshot {
    /// Physical thread executing the operation.
    pub thread_id: ThreadId,
    /// Assigned EventLoop for local execution, or `None` for Worker execution.
    pub event_loop_id: Option<EventLoopId>,
    /// Logical Context of the operation, including the source of blocking work.
    pub context_id: ContextId,
    /// Managed task associated with the operation.
    pub task_id: TaskId,
    /// Operation name such as `future_poll`, `future_drop` or `blocking`.
    pub operation: &'static str,
    /// Time the current synchronous operation began.
    pub started_at: Instant,
}
/// Lifecycle stage of a deployment present at sampling time.
#[derive(Debug, Clone)]
pub struct DeploymentSnapshot {
    /// Identity assigned before the factory was dispatched.
    pub deployment_id: DeploymentId,
    /// Current stage from queueing through cleanup.
    pub stage: &'static str,
}
/// Counts only registered consumer queues. Capacity is in messages, not bytes.
#[derive(Debug, Clone)]
pub struct QueueSnapshot {
    /// Identity of this registration, distinct from later subscribers at the same address.
    pub consumer_id: u64,
    /// EventBus address of the queue.
    pub address: String,
    /// Context that registered the consumer, when bound.
    pub context_id: Option<ContextId>,
    /// Scope responsible for removing this consumer, when bound.
    pub deployment_id: Option<DeploymentId>,
    /// Maximum number of queued messages plus reserved positions.
    pub capacity: usize,
    /// Messages waiting for the consumer to dequeue them.
    pub queued: usize,
    /// Queue positions already held by a sender that has yet to enqueue.
    pub reserved: usize,
    /// Senders waiting for permission to enqueue.
    pub waiting_senders: usize,
    /// Observed full-queue events since this registration began.
    pub full_events: u64,
}
/// Sampling view. Counters include lifecycle drivers and blocking work as tasks;
/// threads lists only in-progress managed poll/drop/blocking operations.
/// Bare Tokio tasks and time spent waiting for the Worker execution gate are
/// not monitored. Successful close leaves all active counts at zero.
#[derive(Debug, Clone, Default)]
pub struct DiagnosticsSnapshot {
    /// Whether the instance collects runtime diagnostics.
    pub enabled: bool,
    /// Process-unique identity of the Vertx instance.
    pub instance_id: u64,
    /// Deployments in startup, running or cleanup.
    pub active_deployments: usize,
    /// Registrations currently present in the address router.
    pub active_consumers: usize,
    /// Messages queued across registered consumers.
    pub queued_messages: usize,
    /// Slots held by senders across registered consumers.
    pub reserved_slots: usize,
    /// Senders waiting for capacity across registered consumers.
    pub waiting_senders: usize,
    /// Cumulative count of observed full queues, including removed consumers.
    pub queue_full_events: u64,
    /// Sampled queue details when enabled.
    pub queues: Vec<QueueSnapshot>,
    /// Whether some registered queues are omitted from the details.
    pub queue_details_truncated: bool,
    /// Managed operations whose tracked state has yet to be destroyed.
    pub active_tasks: usize,
    /// Tasks ready for their next poll.
    pub ready_tasks: usize,
    /// Blocking operations waiting for ordering or a Worker thread.
    pub blocking_waiting: usize,
    /// Blocking operations running on Worker threads.
    pub blocking_running: usize,
    /// Cumulative count of managed operations completed successfully.
    pub completed: u64,
    /// Cumulative count of failed managed operations.
    pub failed: u64,
    /// Cumulative count of cancelled managed operations.
    pub cancelled: u64,
    /// Cumulative count of EventBus requests whose budget expired.
    pub request_timeouts: u64,
    /// Cumulative count of individual broadcast deliveries dropped.
    pub broadcast_dropped: u64,
    /// Instance lifecycle stage, including `closed` after the executor joins.
    pub close_stage: &'static str,
    /// Stage of each deployment still present at the time of sampling.
    pub deployments: Vec<DeploymentSnapshot>,
    /// Bounded set of live operation details when collection is enabled.
    pub tasks: Vec<TaskSnapshot>,
    /// Whether some live tasks are omitted from the details.
    pub task_details_truncated: bool,
    /// Synchronous operations currently executing on managed threads.
    pub threads: Vec<ThreadSnapshot>,
}
impl Diagnostics {
    pub fn snapshot(&self) -> DiagnosticsSnapshot {
        if !self.options.enabled {
            return DiagnosticsSnapshot {
                instance_id: self.instance,
                close_stage: CloseStage::Disabled.as_str(),
                ..DiagnosticsSnapshot::default()
            };
        }
        let (mut out, tasks, queues) = {
            let state = self.state.lock().unwrap();
            let mut out = state.summary.clone();
            out.enabled = true;
            out.instance_id = self.instance;
            out.close_stage = state.close_stage.as_str();
            out.active_deployments = state.deployments.len();
            out.threads = state.threads.values().cloned().collect();
            out.task_details_truncated =
                self.options.task_details && out.active_tasks > state.tasks.len();
            out.deployments = state
                .deployments
                .iter()
                .map(|(&deployment_id, &stage)| DeploymentSnapshot {
                    deployment_id,
                    stage: stage.as_str(),
                })
                .collect();
            let tasks = state.tasks.values().cloned().collect::<Vec<_>>();
            let queues = state.sample_queues(&mut out, &self.options);
            (out, tasks, queues)
        };
        if !tasks.is_empty() {
            let sampled_at = Instant::now();
            out.tasks = tasks
                .into_iter()
                .map(|(mut task, started)| {
                    task.elapsed = sampled_at.duration_since(started);
                    task
                })
                .collect();
            out.tasks.sort_by_key(|task| task.task_id);
        }
        out.queues = queues
            .into_iter()
            .map(super::queue::QueueSample::snapshot)
            .collect();
        out.queues.sort_by_key(|queue| queue.consumer_id);
        out
    }
}
