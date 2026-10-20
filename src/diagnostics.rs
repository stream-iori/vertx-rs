//! Read-only sampling diagnostics. No executor locks, user futures or Contexts
//! are retained here; snapshots are not globally transactional.
use crate::{ContextId, DeploymentId, EventLoopId, TaskId, VertxError};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Lightweight counters and current synchronous operations are enabled by default.
/// Detailed active-task metadata is opt-in and bounded; no history is retained.
#[derive(Debug, Clone)]
pub struct DiagnosticsOptions {
    pub enabled: bool,
    pub task_details: bool,
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
    pub thread_id: std::thread::ThreadId,
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
#[derive(Default)]
struct State {
    summary: DiagnosticsSnapshot,
    details: HashMap<TaskId, (TaskSnapshot, Instant)>,
    threads: HashMap<std::thread::ThreadId, ThreadSnapshot>,
    deployments: HashMap<DeploymentId, &'static str>,
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
            state: Mutex::new(State {
                summary: DiagnosticsSnapshot {
                    close_stage: "running",
                    ..Default::default()
                },
                ..Default::default()
            }),
        })
    }
    pub fn current_task_id(&self) -> Option<TaskId> {
        self.state
            .lock()
            .unwrap()
            .threads
            .get(&std::thread::current().id())
            .map(|thread| thread.task_id)
    }
    pub fn snapshot(&self) -> DiagnosticsSnapshot {
        let state = self.state.lock().unwrap();
        let mut out = state.summary.clone();
        out.enabled = self.options.enabled;
        out.instance_id = self.instance;
        if !out.enabled {
            out.close_stage = "disabled";
        }
        out.tasks = state
            .details
            .values()
            .map(|(s, start)| {
                let mut s = s.clone();
                s.elapsed = start.elapsed();
                s
            })
            .collect();
        out.tasks.sort_by_key(|s| s.task_id);
        out.task_details_truncated =
            self.options.task_details && out.active_tasks > out.tasks.len();
        out.threads = state.threads.values().cloned().collect();
        out.deployments = state
            .deployments
            .iter()
            .map(|(&deployment_id, &stage)| DeploymentSnapshot {
                deployment_id,
                stage,
            })
            .collect();
        out.active_deployments = out.deployments.len();
        out
    }
    pub fn deployment(&self, id: DeploymentId, stage: &'static str) {
        if !self.options.enabled {
            return;
        }
        let mut state = self.state.lock().unwrap();
        if stage == "stopped" {
            state.deployments.remove(&id);
        } else {
            state.deployments.insert(id, stage);
        }
        drop(state);
        tracing::debug!(instance_id = self.instance, deployment_id = %id, stage, "deployment stage");
    }
    pub fn close_stage(&self, stage: &'static str) {
        if !self.options.enabled {
            return;
        }
        self.state.lock().unwrap().summary.close_stage = stage;
        tracing::debug!(instance_id = self.instance, stage, "close stage");
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
    pub fn task(
        self: &Arc<Self>,
        context_id: ContextId,
        deployment_id: DeploymentId,
        stage: TaskStage,
    ) -> TaskRecord {
        let id = TaskId::next();
        let record = TaskRecord {
            diagnostics: self.clone(),
            id,
            context_id,
            deployment_id,
            stage: Arc::new(Mutex::new(StageState {
                phase: Some(stage),
                since: Instant::now(),
            })),
            result: None,
            error_kind: None,
            started: Instant::now(),
        };
        if self.options.enabled {
            let mut s = self.state.lock().unwrap();
            s.summary.active_tasks += 1;
            adjust(&mut s.summary, stage, 1);
            if self.options.task_details && s.details.len() < self.options.max_task_details {
                s.details.insert(
                    id,
                    (
                        TaskSnapshot {
                            task_id: id,
                            context_id,
                            deployment_id,
                            stage,
                            elapsed: Duration::ZERO,
                        },
                        record.started,
                    ),
                );
            }
        }
        record
    }
    pub fn operation(
        self: &Arc<Self>,
        task: &TaskRecord,
        event_loop_id: Option<EventLoopId>,
        operation: &'static str,
        threshold: Duration,
    ) -> OperationGuard {
        let current = ThreadSnapshot {
            thread_id: std::thread::current().id(),
            event_loop_id,
            context_id: task.context_id,
            task_id: task.id,
            operation,
            started_at: Instant::now(),
        };
        let prev = if self.options.enabled {
            self.state
                .lock()
                .unwrap()
                .threads
                .insert(current.thread_id, current.clone())
        } else {
            None
        };
        OperationGuard {
            diagnostics: self.clone(),
            current,
            prev,
            threshold,
        }
    }
}
fn adjust(s: &mut DiagnosticsSnapshot, stage: TaskStage, delta: isize) {
    let counter = match stage {
        TaskStage::Ready => &mut s.ready_tasks,
        TaskStage::BlockingOrderedWait | TaskStage::BlockingQueued => &mut s.blocking_waiting,
        TaskStage::BlockingRunning => &mut s.blocking_running,
        _ => return,
    };
    *counter = counter.saturating_add_signed(delta);
}
struct StageState {
    phase: Option<TaskStage>,
    since: Instant,
}
pub(crate) struct TaskRecord {
    diagnostics: Arc<Diagnostics>,
    pub id: TaskId,
    context_id: ContextId,
    deployment_id: DeploymentId,
    stage: Arc<Mutex<StageState>>,
    result: Option<&'static str>,
    error_kind: Option<&'static str>,
    started: Instant,
}
impl TaskRecord {
    pub fn pending(&mut self) {
        if !self.diagnostics.options.enabled {
            return;
        }
        let mut current = self.stage.lock().unwrap();
        if current.phase == Some(TaskStage::Polling) {
            current.phase = Some(TaskStage::Waiting);
            current.since = Instant::now();
            if let Some((detail, _)) = self
                .diagnostics
                .state
                .lock()
                .unwrap()
                .details
                .get_mut(&self.id)
            {
                detail.stage = TaskStage::Waiting;
            }
        }
    }
    pub fn stage(&mut self, stage: TaskStage) {
        if !self.diagnostics.options.enabled {
            return;
        }
        let mut current = self.stage.lock().unwrap();
        let mut s = self.diagnostics.state.lock().unwrap();
        let ready_wait = if current.phase == Some(TaskStage::Ready) && stage == TaskStage::Polling {
            Some(current.since.elapsed())
        } else {
            None
        };
        current.since = Instant::now();
        if let Some(old) = current.phase.replace(stage) {
            adjust(&mut s.summary, old, -1);
        }
        adjust(&mut s.summary, stage, 1);
        if let Some((detail, _)) = s.details.get_mut(&self.id) {
            detail.stage = stage;
        }
        drop(s);
        drop(current);
        if let Some(ready_queue_wait) = ready_wait {
            tracing::trace!(task_id = %self.id, ?ready_queue_wait, "task dequeued for poll");
        }
    }
    pub fn result<T>(&mut self, result: &crate::Result<T>) {
        self.error_kind = result.as_ref().err().map(VertxError::kind);
        self.result = Some(match result {
            Ok(_) => "completed",
            Err(VertxError::TaskCancelled | VertxError::DeploymentTerminated(_)) => "cancelled",
            Err(_) => "failed",
        });
    }
    pub fn failed(&mut self) {
        self.result = Some("failed");
        self.error_kind = Some("panic");
    }
    pub fn waker(&self, original: &std::task::Waker) -> std::task::Waker {
        if !self.diagnostics.options.enabled {
            return original.clone();
        }
        futures::task::waker(Arc::new(ReadyWake {
            original: original.clone(),
            diagnostics: Arc::downgrade(&self.diagnostics),
            stage: Arc::downgrade(&self.stage),
            id: self.id,
        }))
    }
    pub fn finish(&mut self) {
        let mut current = self.stage.lock().unwrap();
        let Some(stage) = current.phase.take() else {
            return;
        };
        if !self.diagnostics.options.enabled {
            return;
        }
        let mut s = self.diagnostics.state.lock().unwrap();
        s.summary.active_tasks -= 1;
        adjust(&mut s.summary, stage, -1);
        s.details.remove(&self.id);
        let outcome = self.result.unwrap_or(if std::thread::panicking() {
            "failed"
        } else {
            "cancelled"
        });
        match outcome {
            "completed" => s.summary.completed += 1,
            "failed" => s.summary.failed += 1,
            _ => s.summary.cancelled += 1,
        }
        drop(s);
        drop(current);
        tracing::debug!(instance_id = self.diagnostics.instance, deployment_id = %self.deployment_id, context_id = %self.context_id, task_id = %self.id, outcome, error_kind = self.error_kind, total_elapsed = ?self.started.elapsed(), "task finished after user drop");
    }
}
impl Drop for TaskRecord {
    fn drop(&mut self) {
        self.finish();
    }
}
struct ReadyWake {
    original: std::task::Waker,
    diagnostics: std::sync::Weak<Diagnostics>,
    stage: std::sync::Weak<Mutex<StageState>>,
    id: TaskId,
}
impl futures::task::ArcWake for ReadyWake {
    fn wake_by_ref(this: &Arc<Self>) {
        if let (Some(diagnostics), Some(stage)) = (this.diagnostics.upgrade(), this.stage.upgrade())
        {
            let mut current = stage.lock().unwrap();
            if matches!(current.phase, Some(TaskStage::Waiting | TaskStage::Polling))
                && diagnostics.options.enabled
            {
                current.phase = Some(TaskStage::Ready);
                current.since = Instant::now();
                let mut s = diagnostics.state.lock().unwrap();
                s.summary.ready_tasks += 1;
                if let Some((detail, _)) = s.details.get_mut(&this.id) {
                    detail.stage = TaskStage::Ready;
                }
            }
        }
        this.original.wake_by_ref();
    }
}
pub(crate) struct OperationGuard {
    diagnostics: Arc<Diagnostics>,
    current: ThreadSnapshot,
    prev: Option<ThreadSnapshot>,
    threshold: Duration,
}
impl Drop for OperationGuard {
    fn drop(&mut self) {
        if !self.diagnostics.options.enabled {
            return;
        }
        let mut s = self.diagnostics.state.lock().unwrap();
        if let Some(prev) = self.prev.take() {
            s.threads.insert(self.current.thread_id, prev);
        } else {
            s.threads.remove(&self.current.thread_id);
        }
        drop(s);
        let elapsed = self.current.started_at.elapsed();
        if elapsed >= self.threshold {
            tracing::warn!(instance_id = self.diagnostics.instance, context_id = %self.current.context_id, task_id = %self.current.task_id, thread_id = ?self.current.thread_id, event_loop_id = ?self.current.event_loop_id, operation = self.current.operation, synchronous_elapsed = ?elapsed, "slow synchronous operation returned");
        }
    }
}
