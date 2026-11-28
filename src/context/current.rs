use super::Context;
use crate::eventloop::EventLoopId;
use crate::{ExecutionInfo, ExecutorThread, Result, ThreadingModel, VertxError};
use std::cell::{Cell, RefCell};
use std::marker::PhantomData;
use std::mem::DropGuard;
use std::rc::Rc;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ThreadIdentity {
    EventLoop(u64, EventLoopId),
    Worker(u64),
}
thread_local! {
    static CURRENT: RefCell<Option<Context>> = const { RefCell::new(None) };
    static THREAD: Cell<Option<ThreadIdentity>> = const { Cell::new(None) };
}
pub(crate) fn enter_thread(identity: ThreadIdentity) -> impl Drop {
    let previous = THREAD.replace(Some(identity));
    DropGuard::new((previous, PhantomData::<Rc<()>>), |(previous, _)| {
        THREAD.set(previous);
    })
}
pub(crate) fn thread_identity() -> Option<ThreadIdentity> {
    THREAD.with(Cell::get)
}

impl Context {
    pub(crate) fn current() -> Option<Self> {
        CURRENT.with(|c| c.borrow().clone())
    }
    pub(crate) fn execution_info(&self) -> Option<ExecutionInfo> {
        let thread = match thread_identity()? {
            ThreadIdentity::EventLoop(_, id) => ExecutorThread::EventLoop(id),
            ThreadIdentity::Worker(_) => ExecutorThread::Worker,
        };
        let event_loop_id = self.event_loop_id();
        Some(ExecutionInfo {
            context_id: self.id(),
            deployment_id: self.deployment_id(),
            threading_model: if event_loop_id.is_some() {
                ThreadingModel::EventLoop
            } else {
                ThreadingModel::Worker
            },
            event_loop_id,
            thread,
        })
    }

    pub(crate) fn is_on_event_loop(&self) -> bool {
        self.event_loop_id()
            .is_some_and(|id| self.data.executors.event_loops.check_thread(id))
    }
    pub(crate) fn ensure_current_event_loop_context(&self) -> Result<()> {
        self.ensure_accepting_work()?;
        if self.is_on_event_loop() && Self::current().is_some_and(|current| self.same(&current)) {
            return Ok(());
        }
        Err(VertxError::ContextMismatch)
    }

    pub(crate) fn enter(&self) -> ContextGuard {
        let prev = CURRENT.with(|c| c.replace(Some(self.clone())));
        ContextGuard {
            prev,
            _not_send: PhantomData,
        }
    }
}

pub(crate) struct ContextGuard {
    prev: Option<Context>,
    _not_send: PhantomData<Rc<()>>,
}
impl Drop for ContextGuard {
    fn drop(&mut self) {
        let entered = CURRENT.with(|c| c.replace(self.prev.take()));
        drop(entered);
    }
}
