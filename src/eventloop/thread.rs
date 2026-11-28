use super::EventLoopId;
use super::task::Command;
use crate::context::{ThreadIdentity, enter_thread};
use std::io;
use std::sync::Mutex;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::thread::{self, JoinHandle, ThreadId};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

pub(super) struct EventLoop {
    pub(super) admission: Mutex<Option<UnboundedSender<Box<dyn Command>>>>,
    pub(super) thread: ThreadId,
    pub(super) driver: Handle,
    stop: CancellationToken,
}

impl EventLoop {
    pub(super) fn start<F>(
        instance: u64,
        id: EventLoopId,
        runtime_factory: F,
        handles: &mut Vec<JoinHandle<()>>,
    ) -> io::Result<Self>
    where
        F: FnOnce() -> io::Result<Runtime> + Send + 'static,
    {
        let (sender, receiver) = mpsc::unbounded_channel();
        let (ready, startup) = sync_channel(1);
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        let handle = thread::Builder::new()
            .name("vertx-rs-eventloop".into())
            .spawn(move || run(instance, id, runtime_factory, ready, receiver, stopped))?;
        // Keep the handle before the handshake so rollback can join a failed startup.
        handles.push(handle);
        let (thread, driver) = startup
            .recv()
            .map_err(|_| io::Error::other("event loop initialization thread terminated"))??;
        Ok(Self {
            admission: Mutex::new(Some(sender)),
            thread,
            driver,
            stop,
        })
    }

    pub(super) fn request_stop(&self) {
        let sender = self.admission.lock().unwrap().take();
        drop(sender);
        self.stop.cancel();
    }
}

fn run(
    instance: u64,
    id: EventLoopId,
    runtime_factory: impl FnOnce() -> io::Result<Runtime>,
    ready: SyncSender<io::Result<(ThreadId, Handle)>>,
    receiver: UnboundedReceiver<Box<dyn Command>>,
    stopped: CancellationToken,
) {
    let _identity = enter_thread(ThreadIdentity::EventLoop(instance, id));
    let mut receiver = receiver;
    let runtime = match runtime_factory() {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = ready.send(Err(error));
            return;
        }
    };
    let local = LocalSet::new();
    if ready
        .send(Ok((thread::current().id(), runtime.handle().clone())))
        .is_err()
    {
        return;
    }
    local.block_on(&runtime, run_commands(&mut receiver, &stopped));
    drop(local);
    drop(runtime);
}

const COMMAND_BATCH: usize = 64;

async fn run_commands(
    receiver: &mut UnboundedReceiver<Box<dyn Command>>,
    stopped: &CancellationToken,
) {
    let mut batch = 0;
    while let Some(command) = tokio::select! { biased;
        _ = stopped.cancelled() => None,
        command = receiver.recv() => command,
    } {
        command.run();
        batch += 1;
        if batch == COMMAND_BATCH {
            batch = 0;
            tokio::task::yield_now().await;
        }
    }
    receiver.close();
    // Undispatched commands are cancelled on their owning thread.
    while let Ok(command) = receiver.try_recv() {
        drop(command);
    }
}
