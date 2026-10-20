//! Multi-instance deployment options + typed config. Counterpart to
//! `io.vertx.core.DeploymentOptions`.
//!
//! Vert.x's `DeploymentOptions` carries `instances`, `worker`, `config`
//! (JsonObject), and HA settings. We model the bits that are useful in a
//! single-process Rust runtime:
//!
//! - `instances`: how many copies of the verticle to deploy.
//! - `config: Arc<C>`: a typed, shared, immutable config handed to each
//!   factory invocation. Default is `()` so unconfigured deployments stay
//!   ergonomic.
//!
//! We *don't* mirror `worker: bool` — use `Vertx::deploy_worker` instead
//! (explicit > flag). HA / clustering are out of scope.

use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DeploymentOptions<C = ()> {
    instances: usize,
    config: Arc<C>,
}

impl Default for DeploymentOptions<()> {
    fn default() -> Self {
        Self {
            instances: 1,
            config: Arc::new(()),
        }
    }
}

impl DeploymentOptions<()> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<C> DeploymentOptions<C> {
    /// Deploy this many instances of the verticle. Each instance gets its
    /// own `Context` (picked round-robin across EventLoops). Default is 1.
    pub fn instances(mut self, n: usize) -> Self {
        self.instances = n.max(1);
        self
    }

    /// Attach a typed config. Each factory call receives `Arc<C>::clone`.
    pub fn config<D>(self, config: D) -> DeploymentOptions<D> {
        DeploymentOptions {
            instances: self.instances,
            config: Arc::new(config),
        }
    }

    pub fn instances_count(&self) -> usize {
        self.instances
    }

    pub fn shared_config(&self) -> Arc<C> {
        Arc::clone(&self.config)
    }
}
