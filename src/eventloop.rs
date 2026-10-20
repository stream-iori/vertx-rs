use crate::context::{set_thread, ThreadIdentity};
use crate::error::panic_error;
use crate::{Result, TaskHandle, VertxError};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread::JoinHandle;
use tokio::runtime::{Builder, Handle, Runtime};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventLoopId(pub(crate) usize);
impl std::fmt::Display for EventLoopId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "el#{}", self.0)
    }
}

#[derive(Default)]
struct CancelState {
    cancelled: bool,
    finished: bool,
    abort: Option<tokio::task::AbortHandle>,
}
// Only cancellation metadata crosses threads; the non-Send Future stays local.
#[derive(Default)]
pub(crate) struct LocalTaskControl(Mutex<CancelState>);
impl LocalTaskControl {
    pub(crate) fn abort(&self) {
        let abort = {
            let mut state = self.0.lock().unwrap();
            if state.finished {
                return;
            }
            state.cancelled = true;
            state.abort.clone()
        };
        if let Some(abort) = abort {
            abort.abort();
        }
    }
    pub(crate) fn is_finished(&self) -> bool {
        self.0.lock().unwrap().finished
    }
    fn bind(&self, abort: tokio::task::AbortHandle) {
        let cancel = {
            let mut state = self.0.lock().unwrap();
            if state.finished {
                return;
            }
            state.abort = Some(abort.clone());
            state.cancelled
        };
        if cancel {
            abort.abort();
        }
    }
}
struct Completion<T> {
    sender: Option<oneshot::Sender<Result<T>>>,
    control: Arc<LocalTaskControl>,
}
impl<T> Completion<T> {
    fn finish(&mut self, mut output: Result<T>) {
        let (abort, cancelled) = {
            let mut state = self.control.0.lock().unwrap();
            state.finished = true;
            (state.abort.take(), state.cancelled)
        };
        drop(abort);
        if cancelled && output.is_ok() {
            output = Err(VertxError::TaskCancelled);
        }
        if let Some(sender) = self.sender.take() {
            // An abandoned result may itself have a panicking destructor.
            if let Err(error) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = sender.send(output);
            })) {
                tracing::error!(error = %panic_error(error), "local task result destructor panicked");
            }
        }
    }
}
impl<T> Drop for Completion<T> {
    fn drop(&mut self) {
        if self.sender.is_some() {
            self.finish(Err(VertxError::TaskCancelled));
        }
    }
}
struct LocalTask<F: Future<Output = Result<T>>, T> {
    future: Option<Pin<Box<F>>>,
    completion: Completion<T>,
}
impl<F: Future<Output = Result<T>>, T> LocalTask<F, T> {
    fn finish(&mut self, output: Result<T>) {
        let dropped =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(self.future.take())));
        self.completion.finish(match dropped {
            Ok(()) => output,
            Err(error) => Err(panic_error(error)),
        });
    }
}
impl<F: Future<Output = Result<T>>, T> Future for LocalTask<F, T> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            this.future.as_mut().unwrap().as_mut().poll(cx)
        }));
        match result {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(output)) => {
                this.finish(output);
                Poll::Ready(())
            }
            Err(error) => {
                this.finish(Err(panic_error(error)));
                Poll::Ready(())
            }
        }
    }
}
impl<F: Future<Output = Result<T>>, T> Drop for LocalTask<F, T> {
    fn drop(&mut self) {
        if self.future.is_some() {
            self.finish(Err(VertxError::TaskCancelled));
        }
    }
}
trait Command: Send {
    fn run(self: Box<Self>);
}
struct SpawnCommand<F, T> {
    factory: Option<F>,
    completion: Option<Completion<T>>,
}
impl<F, Fut, T> Command for SpawnCommand<F, T>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<T>> + 'static,
    T: Send + 'static,
{
    fn run(mut self: Box<Self>) {
        let factory = self.factory.take().unwrap();
        let mut completion = self.completion.take().unwrap();
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(factory)) {
            Err(error) => completion.finish(Err(panic_error(error))),
            Ok(future) => {
                let control = completion.control.clone();
                let task = LocalTask {
                    future: Some(Box::pin(future)),
                    completion,
                };
                let cancelled = control.0.lock().unwrap().cancelled;
                if cancelled {
                    drop(task);
                } else {
                    let handle = tokio::task::spawn_local(task);
                    // abort() may have raced with creation of the local task.
                    control.bind(handle.abort_handle());
                }
            }
        }
    }
}
impl<F, T> Drop for SpawnCommand<F, T> {
    fn drop(&mut self) {
        if let Some(factory) = self.factory.take() {
            // The receiver drains commands on its owning EventLoop, including
            // commands which were accepted but never dispatched.
            let dropped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(factory)));
            if let Some(mut completion) = self.completion.take() {
                completion.finish(Err(match dropped {
                    Ok(()) => VertxError::TaskCancelled,
                    Err(error) => panic_error(error),
                }));
            }
        }
    }
}
struct EventLoop {
    admission: Mutex<Option<mpsc::UnboundedSender<Box<dyn Command>>>>,
    stop: CancellationToken,
    thread: std::thread::ThreadId,
    driver: Handle,
}
struct ThreadIdentityGuard;
impl Drop for ThreadIdentityGuard {
    fn drop(&mut self) {
        set_thread(None);
    }
}
pub(crate) struct EventLoopPool {
    loops: Vec<EventLoop>,
    handles: Mutex<Vec<JoinHandle<()>>>,
    instance: u64,
    rr: AtomicUsize,
}
impl EventLoopPool {
    pub fn new(size: usize, instance: u64) -> std::io::Result<Self> {
        Self::with_runtime_factory(size, instance, |_| {
            Builder::new_current_thread().enable_all().build()
        })
    }
    fn with_runtime_factory<F>(
        size: usize,
        instance: u64,
        runtime_factory: F,
    ) -> std::io::Result<Self>
    where
        F: Fn(usize) -> std::io::Result<Runtime> + Send + Sync + 'static,
    {
        let runtime_factory = Arc::new(runtime_factory);
        assert!(size > 0, "event loop pool size must be > 0");
        let mut pool = Self {
            loops: Vec::with_capacity(size),
            handles: Mutex::new(Vec::with_capacity(size)),
            instance,
            rr: AtomicUsize::new(0),
        };
        let started = (|| {
            for idx in 0..size {
                let (sender, receiver) = mpsc::unbounded_channel::<Box<dyn Command>>();
                let stop = CancellationToken::new();
                let stopped = stop.clone();
                let (ready, startup) = std::sync::mpsc::sync_channel(1);
                let runtime_factory = runtime_factory.clone();
                let handle = std::thread::Builder::new()
                    .name("vertx-rs-eventloop".into())
                    .spawn(move || {
                        set_thread(Some(ThreadIdentity::EventLoop(instance, EventLoopId(idx))));
                        let _identity = ThreadIdentityGuard;
                        let mut receiver = receiver;
                        let runtime = match runtime_factory(idx) {
                            Ok(runtime) => runtime,
                            Err(error) => {
                                let _ = ready.send(Err(error));
                                return;
                            }
                        };
                        let local = LocalSet::new();
                        if ready
                            .send(Ok((std::thread::current().id(), runtime.handle().clone())))
                            .is_err()
                        {
                            return;
                        }
                        local.block_on(&runtime, async {
                            let mut batch = 0;
                            loop {
                                tokio::select! { biased;
                                    _ = stopped.cancelled() => break,
                                    command = receiver.recv() => match command {
                                        Some(command) => command.run(),
                                        None => break,
                                    },
                                }
                                batch += 1;
                                if batch == 64 {
                                    batch = 0;
                                    tokio::task::yield_now().await;
                                }
                            }
                            receiver.close();
                            while let Ok(command) = receiver.try_recv() {
                                drop(command);
                            }
                        });
                        drop(local);
                        drop(runtime);
                    })?;
                pool.handles.get_mut().unwrap().push(handle);
                let (thread, driver) = startup.recv().map_err(|_| {
                    std::io::Error::other("event loop initialization thread terminated")
                })??;
                pool.loops.push(EventLoop {
                    admission: Mutex::new(Some(sender)),
                    stop,
                    thread,
                    driver,
                });
            }
            Ok::<_, std::io::Error>(())
        })();
        if let Err(error) = started {
            pool.request_stop();
            for handle in pool.handles.get_mut().unwrap().drain(..) {
                let _ = handle.join();
            }
            return Err(error);
        }
        Ok(pool)
    }
    pub fn driver(&self, id: EventLoopId) -> Handle {
        self.loops[id.0].driver.clone()
    }
    pub fn size(&self) -> usize {
        self.loops.len()
    }
    pub fn pick(&self) -> EventLoopId {
        EventLoopId(self.rr.fetch_add(1, Ordering::Relaxed) % self.size())
    }
    pub fn check_thread(&self, id: EventLoopId) -> bool {
        crate::context::thread_identity() == Some(ThreadIdentity::EventLoop(self.instance, id))
            && self.loops[id.0].thread == std::thread::current().id()
    }
    pub fn owns_current_thread(&self) -> bool {
        match crate::context::thread_identity() {
            Some(ThreadIdentity::EventLoop(instance, id)) if instance == self.instance => {
                self.check_thread(id)
            }
            _ => false,
        }
    }
    pub fn spawn_pinned<F, Fut, T>(&self, id: EventLoopId, factory: F) -> Result<TaskHandle<T>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + 'static,
        T: Send + 'static,
    {
        let admission = self.loops[id.0].admission.lock().unwrap();
        let sender = admission.as_ref().ok_or(VertxError::VertxClosed)?;
        if self.check_thread(id) {
            // Defer the factory as well: user code never executes inline.
            drop(admission);
            return Ok(TaskHandle::from_tokio(tokio::task::spawn_local(
                async move { factory().await },
            )));
        }
        let (sender_result, receiver) = oneshot::channel();
        let control = Arc::new(LocalTaskControl::default());
        let command = SpawnCommand {
            factory: Some(factory),
            completion: Some(Completion {
                sender: Some(sender_result),
                control: control.clone(),
            }),
        };
        let sent = sender.send(Box::new(command));
        drop(admission);
        sent.map_err(|_| VertxError::VertxClosed)?;
        Ok(TaskHandle::from_event_loop(receiver, control))
    }
    fn request_stop(&self) {
        for event_loop in &self.loops {
            event_loop.admission.lock().unwrap().take();
            event_loop.stop.cancel();
        }
    }
    // Called as the last phase of close, after all deployments and Workers stop.
    // A temporary reaper can join the coordinating EventLoop without self-wait.
    pub fn finish_shutdown(
        &self,
        mut errors: Vec<VertxError>,
        result: watch::Sender<Option<Result<()>>>,
    ) {
        self.request_stop();
        let handles = std::mem::take(&mut *self.handles.lock().unwrap());
        let failed_result = result.clone();
        let mut failed_errors = errors.clone();
        if let Err(error) = std::thread::Builder::new()
            .name("vertx-rs-close".into())
            .spawn(move || {
                for handle in handles {
                    if let Err(error) = handle.join() {
                        errors.push(panic_error(error));
                    }
                }
                result.send_replace(Some(if errors.is_empty() {
                    Ok(())
                } else {
                    Err(VertxError::CleanupFailed(errors))
                }));
            })
        {
            failed_errors.push(VertxError::Other(format!(
                "failed to start EventLoop reaper: {error}"
            )));
            failed_result.send_replace(Some(Err(VertxError::CleanupFailed(failed_errors))));
        }
    }
}
impl Drop for EventLoopPool {
    fn drop(&mut self) {
        self.request_stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    struct ExitProbe(Arc<AtomicUsize>);
    impl Drop for ExitProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    thread_local! {
        static EXIT_PROBE: RefCell<Option<ExitProbe>> = const { RefCell::new(None) };
    }
    struct OwnerDrop {
        thread: std::thread::ThreadId,
        dropped: Arc<AtomicBool>,
    }
    impl Drop for OwnerDrop {
        fn drop(&mut self) {
            assert_eq!(std::thread::current().id(), self.thread);
            assert!(crate::context::Context::is_on_event_loop_thread());
            self.dropped.store(true, Ordering::Release);
        }
    }
    async fn finish(pool: &EventLoopPool) {
        let (sender, mut receiver) = watch::channel(None);
        pool.finish_shutdown(Vec::new(), sender);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(result) = receiver.borrow().clone() {
                    result.unwrap();
                    return;
                }
                receiver.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
    }
    async fn occupy(pool: &EventLoopPool) -> (std::sync::mpsc::Sender<()>, TaskHandle<()>) {
        let (entered, started) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let task = pool
            .spawn_pinned(EventLoopId(0), move || async move {
                entered.send(()).ok();
                wait.recv().unwrap();
                Ok(())
            })
            .unwrap();
        started.await.unwrap();
        (release, task)
    }
    #[tokio::test]
    async fn queued_abort_drops_before_first_poll_on_the_owning_thread() {
        let pool = EventLoopPool::new(1, 2001).unwrap();
        let (release, occupied) = occupy(&pool).await;
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = OwnerDrop {
            thread: pool.loops[0].thread,
            dropped: dropped.clone(),
        };
        let task = pool
            .spawn_pinned(EventLoopId(0), move || {
                let local = Rc::new(Cell::new(0));
                async move {
                    let _guard = guard;
                    local.set(1);
                    panic!("cancelled future was polled");
                    #[allow(unreachable_code)]
                    Ok(())
                }
            })
            .unwrap();
        let abort = task.abort_handle();
        abort.abort();
        assert!(!abort.is_finished());
        assert!(!dropped.load(Ordering::Acquire));
        release.send(()).unwrap();
        assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
        assert!(abort.is_finished());
        assert!(dropped.load(Ordering::Acquire));
        occupied.await.unwrap();
        finish(&pool).await;
        abort.abort();
    }
    #[tokio::test]
    async fn running_cross_thread_task_can_be_aborted_and_drops_on_owner() {
        let pool = EventLoopPool::new(1, 2002).unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = OwnerDrop {
            thread: pool.loops[0].thread,
            dropped: dropped.clone(),
        };
        let (entered, started) = oneshot::channel();
        let task = pool
            .spawn_pinned(EventLoopId(0), move || async move {
                let _guard = guard;
                entered.send(()).ok();
                std::future::pending::<Result<()>>().await
            })
            .unwrap();
        started.await.unwrap();
        task.abort();
        assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
        assert!(dropped.load(Ordering::Acquire));
        finish(&pool).await;
    }
    #[tokio::test]
    async fn same_thread_dispatch_is_deferred_and_supports_local_futures() {
        let pool = Arc::new(EventLoopPool::new(1, 2003).unwrap());
        let target = pool.clone();
        pool.spawn_pinned(EventLoopId(0), move || async move {
            let local = Rc::new(Cell::new(0));
            let invoked = Arc::new(AtomicBool::new(false));
            let flag = invoked.clone();
            let thread = std::thread::current().id();
            let task = target.spawn_pinned(EventLoopId(0), move || {
                flag.store(true, Ordering::Release);
                let local = Rc::new(Cell::new(0));
                async move {
                    tokio::task::yield_now().await;
                    assert_eq!(std::thread::current().id(), thread);
                    local.set(1);
                    Ok(())
                }
            })?;
            assert!(!invoked.load(Ordering::Acquire));
            local.set(1);
            task.await?;
            assert!(invoked.load(Ordering::Acquire));
            assert_eq!(local.get(), 1);
            Ok(())
        })
        .unwrap()
        .await
        .unwrap();
        finish(&pool).await;
    }
    #[tokio::test]
    async fn shutdown_joins_threads_despite_retained_handles_drivers_and_wakers() {
        let pool = EventLoopPool::new(1, 2004).unwrap();
        let exited = Arc::new(AtomicUsize::new(0));
        let probe = exited.clone();
        let driver = pool.driver(EventLoopId(0));
        let (entered, started) = oneshot::channel();
        let task = pool
            .spawn_pinned(EventLoopId(0), move || {
                EXIT_PROBE.with(|slot| *slot.borrow_mut() = Some(ExitProbe(probe)));
                let mut entered = Some(entered);
                futures::future::poll_fn(move |cx| {
                    if let Some(entered) = entered.take() {
                        entered.send(cx.waker().clone()).ok();
                    }
                    Poll::<Result<()>>::Pending
                })
            })
            .unwrap();
        let wake = started.await.unwrap();
        let abort = task.abort_handle();
        finish(&pool).await;
        assert_eq!(exited.load(Ordering::SeqCst), 1);
        assert!(abort.is_finished());
        assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
        assert!(matches!(
            pool.spawn_pinned(EventLoopId(0), || async { Ok(()) }),
            Err(VertxError::VertxClosed)
        ));
        wake.wake_by_ref();
        abort.abort();
        drop(driver);
    }
    #[tokio::test]
    async fn shutdown_cancels_accepted_commands_on_the_receiver_thread() {
        let pool = EventLoopPool::new(1, 2005).unwrap();
        let (release, occupied) = occupy(&pool).await;
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = OwnerDrop {
            thread: pool.loops[0].thread,
            dropped: dropped.clone(),
        };
        let task = pool
            .spawn_pinned(EventLoopId(0), move || async move {
                let _guard = guard;
                panic!("shutdown command was polled");
                #[allow(unreachable_code)]
                Ok(())
            })
            .unwrap();
        let (done, mut result) = watch::channel(None);
        pool.finish_shutdown(Vec::new(), done);
        release.send(()).unwrap();
        occupied.await.unwrap();
        result.changed().await.unwrap();
        result.borrow().as_ref().unwrap().as_ref().unwrap();
        assert!(matches!(task.await, Err(VertxError::TaskCancelled)));
        assert!(dropped.load(Ordering::Acquire));
    }
    #[tokio::test]
    async fn early_abort_is_forwarded_when_local_abort_handle_is_bound() {
        let control = LocalTaskControl::default();
        // The cancellation bridge must also handle abort-before-bind.
        let task = tokio::spawn(std::future::pending::<()>());
        control.abort();
        control.bind(task.abort_handle());
        assert!(task.await.unwrap_err().is_cancelled());
    }
    #[test]
    fn partial_initialization_failure_joins_all_started_threads() {
        let exited = Arc::new(AtomicUsize::new(0));
        let probe = exited.clone();
        let result = EventLoopPool::with_runtime_factory(3, 2006, move |idx| {
            EXIT_PROBE.with(|slot| *slot.borrow_mut() = Some(ExitProbe(probe.clone())));
            if idx == 1 {
                Err(std::io::Error::other("injected startup failure"))
            } else {
                Builder::new_current_thread().enable_all().build()
            }
        });
        assert!(result.is_err());
        assert_eq!(exited.load(Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn factory_future_and_destructor_panics_do_not_kill_dispatcher() {
        struct PanicDrop;
        impl Drop for PanicDrop {
            fn drop(&mut self) {
                panic!("local destructor panic");
            }
        }
        let pool = EventLoopPool::new(1, 2007).unwrap();
        let task = pool
            .spawn_pinned(EventLoopId(0), || -> std::future::Ready<Result<()>> {
                panic!("factory panic")
            })
            .unwrap();
        assert!(matches!(task.await, Err(VertxError::Panic(_))));
        let task = pool
            .spawn_pinned(EventLoopId(0), || async {
                panic!("future panic");
                #[allow(unreachable_code)]
                Ok(())
            })
            .unwrap();
        assert!(matches!(task.await, Err(VertxError::Panic(_))));
        let (entered, started) = oneshot::channel();
        let task = pool
            .spawn_pinned(EventLoopId(0), move || async move {
                let _guard = PanicDrop;
                entered.send(()).ok();
                std::future::pending::<Result<()>>().await
            })
            .unwrap();
        started.await.unwrap();
        task.abort();
        assert!(matches!(task.await, Err(VertxError::Panic(_))));
        pool.spawn_pinned(EventLoopId(0), || async { Ok(()) })
            .unwrap()
            .await
            .unwrap();
        finish(&pool).await;
    }
    #[tokio::test]
    async fn submissions_racing_with_shutdown_always_finish_or_are_rejected() {
        let pool = Arc::new(EventLoopPool::new(2, 2008).unwrap());
        let barrier = Arc::new(std::sync::Barrier::new(5));
        let mut submitters = Vec::new();
        for n in 0..4 {
            let (target, start) = (pool.clone(), barrier.clone());
            submitters.push(std::thread::spawn(move || {
                let mut tasks = vec![target
                    .spawn_pinned(EventLoopId(n % 2), || async { Ok(()) })
                    .unwrap()];
                start.wait();
                for _ in 0..64 {
                    match target.spawn_pinned(EventLoopId(n % 2), || async { Ok(()) }) {
                        Ok(task) => tasks.push(task),
                        Err(VertxError::VertxClosed) => {}
                        Err(error) => panic!("unexpected admission error: {error}"),
                    }
                }
                tasks
            }));
        }
        barrier.wait();
        let (done, mut result) = watch::channel(None);
        pool.finish_shutdown(Vec::new(), done);
        let mut tasks = Vec::new();
        for submitter in submitters {
            tasks.extend(submitter.join().unwrap());
        }
        if result.borrow().is_none() {
            result.changed().await.unwrap();
        }
        result.borrow().as_ref().unwrap().as_ref().unwrap();
        assert!(tasks.len() >= 4);
        for task in tasks {
            assert!(task.is_finished());
            assert!(matches!(
                task.await,
                Ok(()) | Err(VertxError::TaskCancelled)
            ));
        }
    }
}
