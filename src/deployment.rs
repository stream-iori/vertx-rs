use std::sync::atomic::{AtomicU64, Ordering};

/// Opaque identifier returned by `Vertx::deploy`. Use with `Vertx::undeploy`.
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
