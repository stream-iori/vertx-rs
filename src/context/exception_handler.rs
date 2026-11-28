use crate::error::panic_error;
use crate::{Result, VertxError};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, RwLock};

pub(super) type ExceptionHandler = Arc<dyn Fn(&VertxError) + Send + Sync>;

#[derive(Default)]
pub(crate) struct ExceptionSlot(RwLock<State>);

#[derive(Default)]
struct State {
    handler: Option<ExceptionHandler>,
    sealed: bool,
}

impl ExceptionSlot {
    pub(super) fn snapshot(&self) -> Option<ExceptionHandler> {
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

pub(super) fn drop_handler(handler: Option<ExceptionHandler>) -> Result<()> {
    catch_unwind(AssertUnwindSafe(|| drop(handler))).map_err(panic_error)
}

fn log_drop_failure(result: Result<()>) {
    if let Err(error) = result {
        tracing::error!(error_kind = error.kind(), %error, "exception handler destructor panicked");
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
