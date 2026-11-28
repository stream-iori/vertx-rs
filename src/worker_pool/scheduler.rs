use super::task::Task;
use crate::context::{ThreadIdentity, enter_thread};
use crate::error::panic_error;
use crate::{Result, TaskId, VertxError};
use std::collections::{HashMap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex};

#[derive(Default)]
struct Lane {
    tasks: VecDeque<Arc<Task>>,
    scheduled: bool,
}

enum Dispatch {
    Context(u64),
    Blocking(Box<dyn FnOnce() + Send>),
}

#[derive(Default)]
struct State {
    ready: VecDeque<Dispatch>,
    lanes: HashMap<u64, Lane>,
    tasks: HashMap<TaskId, Arc<Task>>,
    closing: bool,
}

pub(super) struct Scheduler {
    pub(super) instance: u64,
    state: Mutex<State>,
    available: Condvar,
}

impl Scheduler {
    pub(super) fn new(instance: u64) -> Self {
        Self {
            instance,
            state: Mutex::new(State::default()),
            available: Condvar::new(),
        }
    }

    pub(super) fn submit(&self, task: Arc<Task>) -> Result<()> {
        {
            let mut state = self.state.lock().unwrap();
            if state.closing {
                return Err(VertxError::VertxClosed);
            }
            state.tasks.insert(task.id, task.clone());
        }
        task.schedule();
        Ok(())
    }

    pub(super) fn submit_blocking(&self, job: impl FnOnce() + Send + 'static) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.closing {
            return Err(VertxError::VertxClosed);
        }
        state.ready.push_back(Dispatch::Blocking(Box::new(job)));
        self.available.notify_one();
        Ok(())
    }

    // Accepted tasks can still requeue during shutdown.
    pub(super) fn enqueue(&self, task: Arc<Task>) {
        let mut state = self.state.lock().unwrap();
        let id = task.lane;
        let lane = state.lanes.entry(id).or_default();
        lane.tasks.push_back(task);
        if !lane.scheduled {
            lane.scheduled = true;
            state.ready.push_back(Dispatch::Context(id));
        }
        self.available.notify_one();
    }

    pub(super) fn task_finished(&self, id: TaskId) {
        let task = self.state.lock().unwrap().tasks.remove(&id);
        drop(task);
        self.available.notify_all();
    }

    pub(super) fn close(&self) {
        self.state.lock().unwrap().closing = true;
        self.available.notify_all();
    }

    pub(super) fn run(&self) {
        let _identity = enter_thread(ThreadIdentity::Worker(self.instance));
        while let Some(dispatch) = self.next_dispatch() {
            match dispatch {
                Dispatch::Context(id) => self.run_lane(id),
                Dispatch::Blocking(job) => {
                    if let Err(error) = catch_unwind(AssertUnwindSafe(job)) {
                        tracing::error!(error = %panic_error(error), "blocking result destructor panicked");
                    }
                }
            }
        }
    }

    fn next_dispatch(&self) -> Option<Dispatch> {
        let mut state = self.state.lock().unwrap();
        loop {
            if let Some(dispatch) = state.ready.pop_front() {
                return Some(dispatch);
            }
            if state.closing && state.tasks.is_empty() {
                return None;
            }
            state = self.available.wait(state).unwrap();
        }
    }

    fn run_lane(&self, id: u64) {
        let task = self
            .state
            .lock()
            .unwrap()
            .lanes
            .get_mut(&id)
            .expect("scheduled lane exists")
            .tasks
            .pop_front()
            .expect("scheduled lane owns ready work");
        // Keep the lane scheduled until this poll or user destructor returns.
        task.run(self);

        let mut state = self.state.lock().unwrap();
        if state.lanes[&id].tasks.is_empty() {
            state.lanes.remove(&id);
        } else {
            state.ready.push_back(Dispatch::Context(id));
            self.available.notify_one();
        }
    }

    #[cfg(test)]
    pub(super) fn ready_is_empty(&self) -> bool {
        self.state.lock().unwrap().ready.is_empty()
    }
}
