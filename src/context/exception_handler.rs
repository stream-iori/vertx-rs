use super::Context;
use crate::error::panic_error;
use crate::{Result, VertxError};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, RwLock};

type ExceptionHandler = Arc<dyn Fn(&VertxError) + Send + Sync>;

#[derive(Default)]
pub(crate) struct ExceptionSlot(RwLock<State>);

#[derive(Default)]
struct State {
    handler: Option<ExceptionHandler>,
    sealed: bool,
}

impl ExceptionSlot {
    fn snapshot(&self) -> Option<ExceptionHandler> {
        self.0.read().unwrap().handler.clone()
    }

    pub fn set(&self, handler: impl Fn(&VertxError) + Send + Sync + 'static) -> bool {
        self.replace(Some(Arc::new(handler)))
    }

    pub fn clear(&self) -> bool {
        self.replace(None)
    }

    fn replace(&self, handler: Option<ExceptionHandler>) -> bool {
        let (accepted, retired) = {
            let mut state = self.0.write().unwrap();
            if state.sealed {
                (false, handler)
            } else {
                (true, std::mem::replace(&mut state.handler, handler))
            }
        };
        log_drop_failure(drop_handler(retired));
        accepted
    }

    pub fn seal(&self) -> Result<()> {
        let retired = {
            let mut state = self.0.write().unwrap();
            state.sealed = true;
            state.handler.take()
        };
        drop_handler(retired)
    }
}

fn drop_handler(handler: Option<ExceptionHandler>) -> Result<()> {
    catch_unwind(AssertUnwindSafe(|| drop(handler))).map_err(panic_error)
}

fn log_drop_failure(result: Result<()>) {
    if let Err(error) = result {
        tracing::error!(error_kind = error.kind(), %error, "exception handler destructor panicked");
    }
}

impl Context {
    /// Replaces this Context's hook for managed task and lifecycle failures.
    /// Returns whether the exception slot accepted the update.
    pub(crate) fn set_exception_handler<F>(&self, f: F) -> bool
    where
        F: Fn(&VertxError) + Send + Sync + 'static,
    {
        self.data.exception_handler.set(f)
    }
    /// Restores fallback to the owning instance's exception hook.
    pub(crate) fn clear_exception_handler(&self) -> bool {
        self.data.exception_handler.clear()
    }
    pub(crate) fn seal_exception_handler(&self) -> Result<()> {
        self.data.exception_handler.seal()
    }
    pub(crate) fn report_error(&self, err: &VertxError) {
        let handler = self
            .data
            .exception_handler
            .snapshot()
            .or_else(|| self.data.services.exception_handler.snapshot());
        let diagnostics = self.diagnostics();
        let instance_id = diagnostics.instance;
        let deployment_id = self.deployment_id();
        let context_id = self.id();
        if diagnostics.options.enabled {
            tracing::error!(
                instance_id, deployment_id = deployment_id.as_ref().map(tracing::field::display), context_id = %context_id,
                error_kind = err.kind(), error = %err, "framework operation failed"
            );
        }
        if let Some(handler) = handler {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| handler(err))) {
                let error = panic_error(payload);
                tracing::error!(
                    instance_id, deployment_id = deployment_id.as_ref().map(tracing::field::display), context_id = %context_id,
                    error_kind = error.kind(), %error, "exception handler panicked"
                );
            }
            if let Err(error) = drop_handler(Some(handler)) {
                tracing::error!(
                    instance_id, deployment_id = deployment_id.as_ref().map(tracing::field::display), context_id = %context_id,
                    error_kind = error.kind(), %error, "exception handler destructor panicked"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ExceptionSlot;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    struct DropCount(Arc<AtomicUsize>);
    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn concurrent_hook_update_and_seal_release_the_hook_exactly_once() {
        let slot = ExceptionSlot::default();
        let barrier = Barrier::new(2);
        let dropped = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|threads| {
            let lifetime = DropCount(dropped.clone());
            let barrier = &barrier;
            let slot = &slot;
            let update = threads.spawn(move || {
                barrier.wait();
                slot.set(move |_| {
                    std::hint::black_box(&lifetime);
                })
            });
            barrier.wait();
            slot.seal().unwrap();
            update.join().unwrap();
        });
        assert!(slot.snapshot().is_none());
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
    }
}
