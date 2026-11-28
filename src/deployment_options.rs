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

use std::num::NonZeroUsize;
use std::sync::Arc;

/// Configures the number of deployments and their immutable shared configuration.
#[derive(Debug, Clone)]
#[must_use = "options take effect only when passed to an operation"]
pub struct DeploymentOptions<C = ()> {
    instances: NonZeroUsize,
    config: Arc<C>,
}

impl Default for DeploymentOptions<()> {
    fn default() -> Self {
        Self {
            instances: NonZeroUsize::MIN,
            config: Arc::new(()),
        }
    }
}

impl DeploymentOptions<()> {
    /// Creates options for one deployment with unit configuration.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<C> DeploymentOptions<C> {
    /// Deploy this many instances of the verticle. Each instance gets its
    /// own `Context` (picked round-robin across EventLoops). Default is 1.
    ///
    /// ```compile_fail,E0308
    /// use vertx_rs::DeploymentOptions;
    /// DeploymentOptions::new().instances(0);
    /// ```
    pub fn instances(mut self, n: NonZeroUsize) -> Self {
        self.instances = n;
        self
    }

    /// Attach a typed config. Each factory call receives `Arc<C>::clone`.
    pub fn config<D>(self, config: D) -> DeploymentOptions<D> {
        DeploymentOptions {
            instances: self.instances,
            config: Arc::new(config),
        }
    }

    /// Returns the positive number of verticle instances to deploy.
    pub fn instances_count(&self) -> NonZeroUsize {
        self.instances
    }

    /// Clones the shared configuration handle passed to each factory.
    pub fn shared_config(&self) -> Arc<C> {
        Arc::clone(&self.config)
    }
}
