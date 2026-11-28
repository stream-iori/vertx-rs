use crate::eventbus::SubscriptionRegistration;
use crate::task::ActiveTasks;
use crate::tcp::ListenerRegistration;
use crate::{DeploymentId, Result, TaskAbortHandle, TaskId, VertxError};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ScopeOwner {
    Instance,
    Deployment(DeploymentId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ResourceId(pub(crate) u64);

impl ResourceId {
    pub fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

pub(crate) enum ResourceRegistration {
    Subscription(SubscriptionRegistration),
    Listener(ListenerRegistration),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Starting,
    Running,
    Stopping,
}
pub(crate) struct ScopeState {
    phase: Phase,
    tasks: ActiveTasks,
    resources: HashMap<ResourceId, ResourceRegistration>,
}

impl ScopeState {
    pub fn register_resource(&mut self, id: ResourceId, registration: ResourceRegistration) {
        assert!(
            self.resources.insert(id, registration).is_none(),
            "duplicate resource ID"
        );
    }
}
pub(crate) struct WorkScope {
    pub owner: ScopeOwner,
    pub cancel: CancellationToken,
    instance_closing: Arc<AtomicBool>,
    state: Mutex<ScopeState>,
    tracker: TaskTracker,
    blocking: TaskTracker,
}
impl WorkScope {
    pub fn instance() -> Self {
        Self::new(
            ScopeOwner::Instance,
            CancellationToken::new(),
            Arc::default(),
        )
    }

    pub fn deployment(&self, id: DeploymentId) -> Self {
        Self::new(
            ScopeOwner::Deployment(id),
            self.cancel.child_token(),
            self.instance_closing.clone(),
        )
    }

    fn new(
        owner: ScopeOwner,
        cancel: CancellationToken,
        instance_closing: Arc<AtomicBool>,
    ) -> Self {
        let phase = match owner {
            ScopeOwner::Instance => Phase::Running,
            ScopeOwner::Deployment(_) => Phase::Starting,
        };
        Self {
            owner,
            cancel,
            instance_closing,
            state: Mutex::new(ScopeState {
                phase,
                tasks: ActiveTasks::default(),
                resources: HashMap::new(),
            }),
            tracker: TaskTracker::new(),
            blocking: TaskTracker::new(),
        }
    }

    pub fn close_instance_admission(&self) -> bool {
        !self.instance_closing.swap(true, Ordering::AcqRel)
    }

    pub fn instance_closing_flag(&self) -> Arc<AtomicBool> {
        self.instance_closing.clone()
    }

    pub fn instance_admission_closed(&self) -> bool {
        self.instance_closing.load(Ordering::Acquire)
    }

    pub fn terminated(&self) -> VertxError {
        match self.owner {
            ScopeOwner::Instance => VertxError::VertxClosed,
            ScopeOwner::Deployment(id) => VertxError::DeploymentTerminated(id),
        }
    }

    fn ensure_admitted(&self, phase: Phase) -> Result<()> {
        if self.instance_admission_closed() {
            Err(VertxError::VertxClosed)
        } else if self.cancel.is_cancelled() || phase == Phase::Stopping {
            Err(self.terminated())
        } else {
            Ok(())
        }
    }

    pub fn admit(&self) -> Result<std::sync::MutexGuard<'_, ScopeState>> {
        let state = self.state.lock().unwrap();
        self.ensure_admitted(state.phase)?;
        Ok(state)
    }

    pub fn register_task(self: &Arc<Self>, id: TaskId) -> Result<TaskRegistration> {
        let mut state = self.admit()?;
        state.tasks.register(id);
        Ok(TaskRegistration {
            scope: Arc::downgrade(self),
            id,
            _token: self.tracker.token(),
        })
    }

    pub fn attach_task(&self, id: TaskId, handle: TaskAbortHandle) {
        let abort = self.state.lock().unwrap().tasks.attach(id, &handle);
        if abort {
            handle.abort();
        }
    }

    pub fn forget_resource(&self, id: ResourceId) {
        let removed = self.state.lock().unwrap().resources.remove(&id);
        drop(removed);
    }

    /// The scope has stopped admitting resources before lifecycle cleanup calls this.
    pub fn unregister_resources(&self) {
        let registrations = std::mem::take(&mut self.state.lock().unwrap().resources);
        for registration in registrations.into_values() {
            match registration {
                ResourceRegistration::Subscription(registration) => registration.unregister(),
                ResourceRegistration::Listener(registration) => {
                    registration.unregister();
                }
            }
        }
    }

    pub fn admit_blocking(&self, in_stop_callback: bool) -> Result<TaskTrackerToken> {
        let state = self.state.lock().unwrap();
        if !(in_stop_callback && state.phase == Phase::Stopping) {
            self.ensure_admitted(state.phase)?;
        }
        Ok(self.blocking.token())
    }

    pub fn try_mark_running(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.phase == Phase::Starting && self.ensure_admitted(state.phase).is_ok() {
            state.phase = Phase::Running;
            true
        } else {
            false
        }
    }

    pub fn request_stop(&self) {
        self.state.lock().unwrap().phase = Phase::Stopping;
        // Closing trackers and cancelling the token can wake other tasks.
        self.tracker.close();
        self.blocking.close();
        self.cancel.cancel();
    }
    pub async fn drain_tasks(&self, grace: Duration) {
        if tokio::time::timeout(grace / 2, self.tracker.wait())
            .await
            .is_err()
        {
            self.abort_tasks();
            self.tracker.wait().await;
        }
    }
    pub fn abort_tasks(&self) {
        let tasks = self.state.lock().unwrap().tasks.take_abort_handles();
        for task in tasks {
            task.abort();
        }
    }

    pub async fn wait_blocking(&self) {
        self.blocking.wait().await;
    }
    pub fn task_token(&self) -> TaskTrackerToken {
        self.tracker.token()
    }
}

pub(crate) struct TaskRegistration {
    scope: Weak<WorkScope>,
    id: TaskId,
    _token: TaskTrackerToken,
}

impl Drop for TaskRegistration {
    fn drop(&mut self) {
        if let Some(scope) = self.scope.upgrade() {
            let handle = scope.state.lock().unwrap().tasks.remove(self.id);
            drop(handle);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DeploymentId, TaskId, WorkScope};
    use crate::TaskHandle;
    use crate::task::TaskControl;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio::sync::oneshot;

    #[derive(Default)]
    struct AbortProbe(AtomicBool);

    impl TaskControl for AbortProbe {
        fn abort(&self) {
            self.0.store(true, Ordering::Relaxed);
        }

        fn is_finished(&self) -> bool {
            false
        }
    }

    #[test]
    fn instance_closure_reaches_deployment_admission_before_cancellation() {
        let instance = WorkScope::instance();
        let deployment = instance.deployment(DeploymentId::next());
        assert!(instance.close_instance_admission());
        assert!(!instance.close_instance_admission());
        assert!(!deployment.cancel.is_cancelled());
        assert!(matches!(
            instance.admit(),
            Err(crate::VertxError::VertxClosed)
        ));
        assert!(matches!(
            deployment.admit(),
            Err(crate::VertxError::VertxClosed)
        ));
        assert!(!deployment.try_mark_running());
        assert!(deployment.admit_blocking(true).is_err());

        deployment.request_stop();
        assert!(deployment.admit_blocking(false).is_err());
        assert!(deployment.admit_blocking(true).is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn draining_aborts_a_late_handle_but_ignores_completed_tasks() {
        let scope = Arc::new(WorkScope::instance().deployment(DeploymentId::next()));
        let pending = TaskId::next();
        let finished = TaskId::next();
        let registration = scope.register_task(pending).unwrap();
        drop(scope.register_task(finished).unwrap());
        scope.request_stop();
        let grace = Duration::from_secs(2);
        let drain = scope.drain_tasks(grace);
        tokio::pin!(drain);
        assert!(futures::poll!(&mut drain).is_pending());
        tokio::time::advance(grace).await;
        assert!(futures::poll!(&mut drain).is_pending());

        let pending_probe = Arc::new(AbortProbe::default());
        let pending_handle =
            TaskHandle::<()>::from_channel(pending, oneshot::channel().1, pending_probe.clone());
        scope.attach_task(pending, pending_handle.abort_handle());
        assert!(pending_probe.0.load(Ordering::Relaxed));

        let finished_probe = Arc::new(AbortProbe::default());
        let finished_handle =
            TaskHandle::<()>::from_channel(finished, oneshot::channel().1, finished_probe.clone());
        scope.attach_task(finished, finished_handle.abort_handle());
        assert!(!finished_probe.0.load(Ordering::Relaxed));
        drop(registration);
        drain.await;
    }
}
