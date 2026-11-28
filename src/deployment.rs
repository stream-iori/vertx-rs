use crate::eventbus::SubscriptionRegistration;
use crate::task::ActiveTasks;
use crate::tcp::ListenerRegistration;
use crate::{Result, TaskAbortHandle, TaskId, VertxError};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;

/// Process-unique deployment identifier, assigned before the factory runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeploymentId(pub(crate) u64);
static NEXT: AtomicU64 = AtomicU64::new(1);
impl DeploymentId {
    pub(crate) fn next() -> Self {
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}
impl std::fmt::Display for DeploymentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "deployment#{}", self.0)
    }
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
    Stopped,
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
pub(crate) struct DeploymentScope {
    pub deployment: DeploymentId,
    pub cancel: CancellationToken,
    state: Mutex<ScopeState>,
    tracker: TaskTracker,
    blocking: TaskTracker,
    done: watch::Sender<Option<Result<()>>>,
}
impl DeploymentScope {
    pub fn new(deployment: DeploymentId, cancel: CancellationToken) -> Self {
        Self {
            deployment,
            cancel,
            state: Mutex::new(ScopeState {
                phase: Phase::Starting,
                tasks: ActiveTasks::default(),
                resources: HashMap::new(),
            }),
            tracker: TaskTracker::new(),
            blocking: TaskTracker::new(),
            done: watch::channel(None).0,
        }
    }
    pub fn admit(&self) -> Result<std::sync::MutexGuard<'_, ScopeState>> {
        let state = self.state.lock().unwrap();
        if self.cancel.is_cancelled() || matches!(state.phase, Phase::Stopping | Phase::Stopped) {
            Err(VertxError::DeploymentTerminated(self.deployment))
        } else {
            Ok(state)
        }
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

    pub fn admit_blocking(
        &self,
        instance_cancel: &CancellationToken,
        in_stop_callback: bool,
    ) -> Result<TaskTrackerToken> {
        let state = self.state.lock().unwrap();
        let running = matches!(state.phase, Phase::Starting | Phase::Running)
            && !instance_cancel.is_cancelled()
            && !self.cancel.is_cancelled();
        let cleanup = in_stop_callback && state.phase == Phase::Stopping;
        if running || cleanup {
            Ok(self.blocking.token())
        } else {
            Err(VertxError::DeploymentTerminated(self.deployment))
        }
    }
    pub fn running(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.phase == Phase::Starting && !self.cancel.is_cancelled() {
            state.phase = Phase::Running;
            true
        } else {
            false
        }
    }
    pub fn request_stop(&self) {
        {
            let mut state = self.state.lock().unwrap();
            if state.phase != Phase::Stopped {
                state.phase = Phase::Stopping;
            }
        }
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
            let tasks = self.state.lock().unwrap().tasks.take_abort_handles();
            for task in &tasks {
                task.abort();
            }
            self.tracker.wait().await;
        }
    }
    pub async fn wait_blocking(&self) {
        self.blocking.wait().await;
    }
    pub fn finish(&self, outcome: Result<()>) {
        self.state.lock().unwrap().phase = Phase::Stopped;
        self.done.send_replace(Some(outcome));
    }
    pub async fn wait(&self) -> Result<()> {
        let mut rx = self.done.subscribe();
        loop {
            if let Some(result) = rx.borrow().clone() {
                return result;
            }
            if rx.changed().await.is_err() {
                return Err(VertxError::Other("deployment completion closed".into()));
            }
        }
    }
    pub async fn wait_with_grace(&self, grace: Duration) -> Result<()> {
        tokio::time::timeout(grace, self.wait())
            .await
            .map_err(|_| VertxError::ShutdownTimeout(grace))?
    }
    pub fn task_token(&self) -> TaskTrackerToken {
        self.tracker.token()
    }
}

pub(crate) struct TaskRegistration {
    scope: Weak<DeploymentScope>,
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
    use super::{DeploymentId, DeploymentScope, TaskId};
    use crate::TaskHandle;
    use crate::task::TaskControl;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio::sync::oneshot;
    use tokio_util::sync::CancellationToken;

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

    #[tokio::test(start_paused = true)]
    async fn draining_aborts_a_late_handle_but_ignores_completed_tasks() {
        let scope = Arc::new(DeploymentScope::new(
            DeploymentId::next(),
            CancellationToken::new(),
        ));
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
