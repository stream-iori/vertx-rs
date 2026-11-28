use super::{Context, Target};
use crate::{Result, TaskHandle, TaskId, VertxError};
use std::future::Future;

impl Context {
    pub(crate) fn spawn_local<F, T>(&self, future: F) -> Result<TaskHandle<T>>
    where
        F: Future<Output = Result<T>> + 'static,
        T: 'static,
    {
        self.spawn_local_with_id(TaskId::next(), future)
    }

    pub(crate) fn spawn_local_with_id<F, T>(&self, id: TaskId, future: F) -> Result<TaskHandle<T>>
    where
        F: Future<Output = Result<T>> + 'static,
        T: 'static,
    {
        self.ensure_current_event_loop_context()?;
        let registration = self.scope().register_task(id)?;
        let frame = self.frame(id);
        let task = self.clone().track_future(frame, registration, future);
        let handle = TaskHandle::from_tokio(id, tokio::task::spawn_local(task));
        self.scope().attach_task(id, handle.abort_handle());
        Ok(handle)
    }

    pub(crate) fn spawn<F, T>(&self, future: F) -> Result<TaskHandle<T>>
    where
        F: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        if let Some(id) = self.event_loop_id()
            && !(self.data.executors.event_loops.check_thread(id) && Self::current().is_some())
        {
            return self.spawn_pinned(move || future);
        }
        let id = TaskId::next();
        let registration = self.scope().register_task(id)?;
        let task = self
            .clone()
            .track_future(self.frame(id), registration, future);
        let handle = match self.data.target {
            Target::EventLoop(_) => TaskHandle::from_tokio(id, tokio::task::spawn_local(task)),
            Target::Worker { .. } => self.spawn_worker(id, task)?,
        };
        self.scope().attach_task(id, handle.abort_handle());
        Ok(handle)
    }

    pub(crate) fn spawn_pinned<F, Fut, T>(&self, factory: F) -> Result<TaskHandle<T>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + 'static,
        T: Send + 'static,
    {
        let loop_id = self.event_loop_id().ok_or(VertxError::ContextMismatch)?;
        let id = TaskId::next();
        let registration = self.scope().register_task(id)?;
        let frame = self.frame(id);
        let ctx = self.clone();
        let handle = self
            .data
            .executors
            .event_loops
            .spawn_pinned(loop_id, id, move || {
                ctx.track_future(frame, registration, factory())
            })?;
        self.scope().attach_task(id, handle.abort_handle());
        Ok(handle)
    }

    pub(crate) fn spawn_worker<F, T>(&self, id: TaskId, task: F) -> Result<TaskHandle<T>>
    where
        F: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        let Target::Worker { driver, .. } = &self.data.target else {
            unreachable!("worker task requires a Worker Context");
        };
        self.data
            .executors
            .workers
            .spawn(id, self.data.id.0, driver.clone(), task)
    }
}
