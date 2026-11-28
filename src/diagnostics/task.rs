use super::{Diagnostics, DiagnosticsSnapshot, State, TaskSnapshot, TaskStage, ThreadSnapshot};
use crate::{ContextId, DeploymentId, EventLoopId, TaskId, VertxError};
use futures::task::{ArcWake, waker};
use std::sync::{Arc, Mutex, Weak};
use std::task::Waker;
use std::time::{Duration, Instant};

impl Diagnostics {
    pub fn current_task_id(&self) -> Option<TaskId> {
        self.state
            .lock()
            .unwrap()
            .threads
            .get(&std::thread::current().id())
            .map(|thread| thread.task_id)
    }
    pub fn task(
        self: &Arc<Self>,
        id: TaskId,
        context_id: ContextId,
        deployment_id: Option<DeploymentId>,
        stage: TaskStage,
    ) -> TaskRecord {
        let record = TaskRecord {
            diagnostics: self.clone(),
            id,
            context_id,
            deployment_id,
            stage: Arc::new(Mutex::new(StageState {
                phase: Some(stage),
                since: Instant::now(),
            })),
            outcome: None,
            error_kind: None,
            started: Instant::now(),
        };
        if self.options.enabled {
            let mut state = self.state.lock().unwrap();
            state.summary.active_tasks += 1;
            adjust(&mut state.summary, stage, 1);
            if self.options.task_details && state.tasks.len() < self.options.max_task_details {
                state.tasks.insert(
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
fn adjust(snapshot: &mut DiagnosticsSnapshot, stage: TaskStage, delta: isize) {
    let counter = match stage {
        TaskStage::Ready => &mut snapshot.ready_tasks,
        TaskStage::BlockingOrderedWait | TaskStage::BlockingQueued => {
            &mut snapshot.blocking_waiting
        }
        TaskStage::BlockingRunning => &mut snapshot.blocking_running,
        _ => return,
    };
    *counter = counter.saturating_add_signed(delta);
}

impl State {
    fn update_task_stage(&mut self, id: TaskId, previous: Option<TaskStage>, stage: TaskStage) {
        if let Some(previous) = previous {
            adjust(&mut self.summary, previous, -1);
        }
        adjust(&mut self.summary, stage, 1);
        if let Some((detail, _)) = self.tasks.get_mut(&id) {
            detail.stage = stage;
        }
    }
}

struct StageState {
    phase: Option<TaskStage>,
    since: Instant,
}

#[derive(Clone, Copy)]
enum TaskOutcome {
    Completed,
    Failed,
    Cancelled,
}

impl TaskOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

pub(crate) struct TaskRecord {
    diagnostics: Arc<Diagnostics>,
    pub id: TaskId,
    context_id: ContextId,
    deployment_id: Option<DeploymentId>,
    stage: Arc<Mutex<StageState>>,
    outcome: Option<TaskOutcome>,
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
            let previous = current.phase.replace(TaskStage::Waiting);
            current.since = Instant::now();
            self.diagnostics.state.lock().unwrap().update_task_stage(
                self.id,
                previous,
                TaskStage::Waiting,
            );
        }
    }
    pub fn stage(&mut self, stage: TaskStage) {
        if !self.diagnostics.options.enabled {
            return;
        }
        let mut current = self.stage.lock().unwrap();
        let mut state = self.diagnostics.state.lock().unwrap();
        let ready_wait = if current.phase == Some(TaskStage::Ready) && stage == TaskStage::Polling {
            Some(current.since.elapsed())
        } else {
            None
        };
        current.since = Instant::now();
        let previous = current.phase.replace(stage);
        state.update_task_stage(self.id, previous, stage);
        drop(state);
        drop(current);
        if let Some(ready_queue_wait) = ready_wait {
            tracing::trace!(task_id = %self.id, ?ready_queue_wait, "task dequeued for poll");
        }
    }
    pub fn result<T>(&mut self, result: &crate::Result<T>, cancelled: bool) {
        self.error_kind = result.as_ref().err().map(VertxError::kind);
        self.outcome = Some(match result {
            Ok(_) => TaskOutcome::Completed,
            Err(_) if cancelled => TaskOutcome::Cancelled,
            Err(_) => TaskOutcome::Failed,
        });
    }
    pub fn failed(&mut self) {
        self.outcome = Some(TaskOutcome::Failed);
        self.error_kind = Some("panic");
    }
    pub fn waker(&self, original: &Waker) -> Waker {
        if !self.diagnostics.options.enabled {
            return original.clone();
        }
        waker(Arc::new(ReadyWake {
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
        let mut state = self.diagnostics.state.lock().unwrap();
        state.summary.active_tasks -= 1;
        adjust(&mut state.summary, stage, -1);
        state.tasks.remove(&self.id);
        let outcome = self.outcome.unwrap_or_else(|| {
            if std::thread::panicking() {
                TaskOutcome::Failed
            } else {
                TaskOutcome::Cancelled
            }
        });
        match outcome {
            TaskOutcome::Completed => state.summary.completed += 1,
            TaskOutcome::Failed => state.summary.failed += 1,
            TaskOutcome::Cancelled => state.summary.cancelled += 1,
        }
        drop(state);
        drop(current);
        tracing::debug!(
            instance_id = self.diagnostics.instance, deployment_id = self.deployment_id.as_ref().map(tracing::field::display),
            context_id = %self.context_id, task_id = %self.id, outcome = outcome.as_str(),
            error_kind = self.error_kind, total_elapsed = ?self.started.elapsed(),
            "task finished after user drop"
        );
    }
}
impl Drop for TaskRecord {
    fn drop(&mut self) {
        self.finish();
    }
}
struct ReadyWake {
    original: Waker,
    diagnostics: Weak<Diagnostics>,
    stage: Weak<Mutex<StageState>>,
    id: TaskId,
}
impl ArcWake for ReadyWake {
    fn wake_by_ref(this: &Arc<Self>) {
        this.mark_ready();
        this.original.wake_by_ref();
    }
}

impl ReadyWake {
    fn mark_ready(&self) {
        let (Some(diagnostics), Some(stage)) = (self.diagnostics.upgrade(), self.stage.upgrade())
        else {
            return;
        };
        let mut current = stage.lock().unwrap();
        if matches!(current.phase, Some(TaskStage::Waiting | TaskStage::Polling))
            && diagnostics.options.enabled
        {
            let previous = current.phase.replace(TaskStage::Ready);
            current.since = Instant::now();
            diagnostics.state.lock().unwrap().update_task_stage(
                self.id,
                previous,
                TaskStage::Ready,
            );
        }
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
        let mut state = self.diagnostics.state.lock().unwrap();
        if let Some(prev) = self.prev.take() {
            state.threads.insert(self.current.thread_id, prev);
        } else {
            state.threads.remove(&self.current.thread_id);
        }
        drop(state);
        let elapsed = self.current.started_at.elapsed();
        if elapsed >= self.threshold {
            tracing::warn!(
                instance_id = self.diagnostics.instance, context_id = %self.current.context_id,
                task_id = %self.current.task_id, thread_id = ?self.current.thread_id,
                event_loop_id = ?self.current.event_loop_id, operation = self.current.operation,
                synchronous_elapsed = ?elapsed, "slow synchronous operation returned"
            );
        }
    }
}
