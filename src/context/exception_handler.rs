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

    pub fn set(&self, handler: impl Fn(&VertxError) + Send + Sync + 'static) {
        let handler: ExceptionHandler = Arc::new(handler);
        let retired = {
            let mut state = self.0.write().unwrap();
            if state.sealed {
                Some(handler)
            } else {
                state.handler.replace(handler)
            }
        };
        log_drop_failure(drop_handler(retired));
    }

    pub fn clear(&self) {
        let retired = self.0.write().unwrap().handler.take();
        log_drop_failure(drop_handler(retired));
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
