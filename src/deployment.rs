use crate::TaskAbortHandle;
use crate::{Result, VertxError};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Starting,
    Running,
    Stopping,
    Stopped,
}
pub(crate) struct ScopeState {
    pub phase: Phase,
    pub tasks: Vec<TaskAbortHandle>,
}
pub(crate) struct DeploymentScope {
    pub deployment: DeploymentId,
    pub cancel: CancellationToken,
    pub state: Mutex<ScopeState>,
    tracker: TaskTracker,
    pub blocking: TaskTracker,
    done: watch::Sender<Option<Result<()>>>,
}
impl DeploymentScope {
    pub fn new(deployment: DeploymentId, cancel: CancellationToken) -> Self {
        Self {
            deployment,
            cancel,
            state: Mutex::new(ScopeState {
                phase: Phase::Starting,
                tasks: Vec::new(),
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
    pub fn is_alive(&self) -> bool {
        self.admit().is_ok()
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
        let timed_out = tokio::time::timeout(grace / 2, self.tracker.wait())
            .await
            .is_err();
        let tasks = std::mem::take(&mut self.state.lock().unwrap().tasks);
        if timed_out {
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
