use super::support::{local, report_on_thread_exit, worker};
use rstest::rstest;
use std::cell::RefCell;
use std::future::{Future, pending, poll_fn};
use std::io;
use std::net::SocketAddr;
use std::pin::{Pin, pin};
use std::task::Poll;
use std::thread::{self, ThreadId};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Barrier, oneshot};
use vertx_rs::{
    Deployment, EventLoopId, ExecutionInfo, ExecutorThread, Result, TaskHandle, TcpIncoming, Vertx,
    VertxError, VertxHandle,
};

const EPHEMERAL: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 0);
const TIMEOUT: Duration = Duration::from_secs(5);

thread_local! {
    static RETAINED: RefCell<Option<TcpIncoming>> = const { RefCell::new(None) };
}

fn instance(loops: usize) -> Vertx {
    Vertx::builder()
        .event_loop_threads(loops)
        .worker_threads(1)
        .build()
        .unwrap()
}

async fn serve<F, Fut>(
    vx: &Vertx,
    addr: SocketAddr,
    run: F,
) -> (Deployment, SocketAddr, TaskHandle<()>)
where
    F: FnOnce(VertxHandle, TcpIncoming) -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + 'static,
{
    let (ready, started) = oneshot::channel();
    let deployment = vx
        .deploy(move || {
            local(move |ctx| async move {
                let incoming = ctx.listen_tcp(addr).await?;
                let addr = incoming.local_addr();
                let owner = ctx.clone();
                let task = ctx.spawn_local(async move { run(owner, incoming).await })?;
                ready
                    .send((addr, task))
                    .map_err(|_| VertxError::ReplyClosed)
            })
        })
        .await
        .unwrap();
    let (addr, task) = started.await.unwrap();
    (deployment, addr, task)
}

async fn confirm_pending(mut future: Pin<&mut impl Future>) {
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

fn assert_owner(expected: ExecutionInfo, thread: ThreadId) {
    assert_eq!(ExecutionInfo::current(), Some(expected));
    assert_eq!(std::thread::current().id(), thread);
    assert!(matches!(expected.thread, ExecutorThread::EventLoop(_)));
}

async fn reply(mut socket: TcpStream, tag: u8) {
    let request = socket.read_u8().await.unwrap();
    socket.write_all(&[tag, request]).await.unwrap();
}

async fn request(addr: SocketAddr, tag: u8) {
    let mut socket = TcpStream::connect(addr).await.unwrap();
    socket.write_u8(42).await.unwrap();
    let mut response = [0; 2];
    tokio::time::timeout(TIMEOUT, socket.read_exact(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response, [tag, 42]);
}

async fn waiting_echo(
    ctx: VertxHandle,
    mut incoming: TcpIncoming,
    tag: u8,
    ready: oneshot::Sender<(EventLoopId, ThreadId)>,
) -> Result<()> {
    let thread = thread::current().id();
    let execution = ctx.current_execution().unwrap();
    assert_owner(execution, thread);
    let mut accept = pin!(incoming.accept());
    confirm_pending(accept.as_mut()).await;
    ready
        .send((execution.event_loop_id.unwrap(), thread))
        .unwrap();
    let (socket, peer) = accept.await?;
    assert!(peer.ip().is_loopback());
    assert_owner(execution, thread);
    reply(socket, tag).await;
    assert_owner(execution, thread);
    Ok(())
}

#[tokio::test]
async fn shared_listener_serves_waiters_in_order_on_distinct_event_loops() {
    let vx = instance(2);
    let (first_ready, first_waiting) = oneshot::channel();
    let (_, addr, first) = serve(&vx, EPHEMERAL, |ctx, incoming| {
        waiting_echo(ctx, incoming, 0, first_ready)
    })
    .await;
    let first_owner = first_waiting.await.unwrap();

    let (second_ready, second_waiting) = oneshot::channel();
    let (_, second_addr, second) = serve(&vx, addr, |ctx, incoming| {
        waiting_echo(ctx, incoming, 1, second_ready)
    })
    .await;
    let second_owner = second_waiting.await.unwrap();
    assert_eq!(addr, second_addr);
    assert_ne!(first_owner.0, second_owner.0);
    assert_ne!(first_owner.1, second_owner.1);

    request(addr, 0).await;
    request(addr, 1).await;
    first.await.unwrap();
    second.await.unwrap();
    vx.close().await.unwrap();
}

#[tokio::test]
async fn busy_registration_is_skipped_until_it_waits_again() {
    let vx = instance(2);
    let (busy, first_accepted) = oneshot::channel();
    let (resume, finish_first) = oneshot::channel();
    let (_, addr, first) = serve(&vx, EPHEMERAL, |_, mut incoming| async move {
        let (socket, _) = incoming.accept().await?;
        busy.send(()).unwrap();
        finish_first.await.unwrap();
        reply(socket, 0).await;
        Ok(())
    })
    .await;
    let mut first_client = TcpStream::connect(addr).await.unwrap();
    first_accepted.await.unwrap();

    let (_, _, second) = serve(&vx, addr, |_, mut incoming| async move {
        let (socket, _) = incoming.accept().await?;
        reply(socket, 1).await;
        Ok(())
    })
    .await;
    request(addr, 1).await;
    resume.send(()).unwrap();
    first_client.write_u8(77).await.unwrap();
    assert_eq!(
        tokio::time::timeout(TIMEOUT, first_client.read_u16())
            .await
            .unwrap()
            .unwrap(),
        77
    );
    first.await.unwrap();
    second.await.unwrap();
    vx.close().await.unwrap();
}

#[rstest]
#[case::head(true)]
#[case::queued(false)]
#[tokio::test]
async fn cancelling_accept_releases_its_place_and_allows_retry(#[case] cancel_head: bool) {
    let vx = instance(2);
    let (candidate_ready, candidate_waiting) = oneshot::channel();
    let (cancel, cancelled) = oneshot::channel();
    let (released, turn_released) = oneshot::channel();
    let (retry, retrying) = oneshot::channel();
    let (peer_ready, peer_waiting) = oneshot::channel();
    let candidate = |_: VertxHandle, mut incoming: TcpIncoming| async move {
        {
            let mut accept = pin!(incoming.accept());
            confirm_pending(accept.as_mut()).await;
            candidate_ready.send(()).unwrap();
            tokio::select! {
                result = accept => panic!("accept finished before cancellation: {result:?}"),
                result = cancelled => result.unwrap(),
            }
        }
        released.send(()).unwrap();
        retrying.await.unwrap();
        let (socket, _) = incoming.accept().await?;
        reply(socket, 0).await;
        Ok(())
    };
    let peer = |ctx, incoming| waiting_echo(ctx, incoming, 1, peer_ready);
    let (addr, candidate, peer) = if cancel_head {
        let (_, addr, candidate) = serve(&vx, EPHEMERAL, candidate).await;
        candidate_waiting.await.unwrap();
        let (_, _, peer) = serve(&vx, addr, peer).await;
        peer_waiting.await.unwrap();
        (addr, candidate, peer)
    } else {
        let (_, addr, peer) = serve(&vx, EPHEMERAL, peer).await;
        peer_waiting.await.unwrap();
        let (_, _, candidate) = serve(&vx, addr, candidate).await;
        candidate_waiting.await.unwrap();
        (addr, candidate, peer)
    };
    cancel.send(()).unwrap();
    turn_released.await.unwrap();
    request(addr, 1).await;
    retry.send(()).unwrap();
    request(addr, 0).await;
    candidate.await.unwrap();
    peer.await.unwrap();
    vx.close().await.unwrap();
}

#[tokio::test]
async fn worker_rejects_managed_listener_before_binding() {
    let vx = instance(1);
    let reservation = std::net::TcpListener::bind(EPHEMERAL).unwrap();
    let occupied = reservation.local_addr().unwrap();
    vx.deploy_worker(move || {
        worker(move |ctx| async move {
            assert!(matches!(
                ctx.listen_tcp(occupied).await,
                Err(VertxError::ContextMismatch)
            ));
            assert!(matches!(
                ctx.listen_tcp(EPHEMERAL).await,
                Err(VertxError::ContextMismatch)
            ));
            Ok(())
        })
    })
    .await
    .unwrap();
    drop(reservation);
    let rebound = std::net::TcpListener::bind(occupied).unwrap();
    drop(rebound);
    vx.close().await.unwrap();
}

#[tokio::test]
async fn different_context_on_same_event_loop_cannot_accept() {
    let vx = instance(1);
    let (owner, owning_thread) = oneshot::channel();
    vx.deploy(move || {
        local(move |ctx| async move {
            retain(ctx.listen_tcp(EPHEMERAL).await?);
            owner
                .send((
                    ctx.current_execution().unwrap().context_id,
                    thread::current().id(),
                ))
                .unwrap();
            Ok(())
        })
    })
    .await
    .unwrap();
    let (id, thread) = owning_thread.await.unwrap();
    vx.deploy(move || {
        local(move |ctx| async move {
            assert_ne!(ctx.current_execution().unwrap().context_id, id);
            assert_eq!(std::thread::current().id(), thread);
            let mut incoming = RETAINED.take().unwrap();
            assert!(matches!(
                incoming.accept().await,
                Err(VertxError::ContextMismatch)
            ));
            Ok(())
        })
    })
    .await
    .unwrap();
    vx.close().await.unwrap();
}

#[tokio::test]
async fn occupied_port_and_duplicate_registration_report_distinct_errors() {
    let vx = instance(1);
    let occupied = std::net::TcpListener::bind(EPHEMERAL).unwrap();
    let addr = occupied.local_addr().unwrap();
    vx.deploy(move || {
        local(move |ctx| async move {
            assert!(matches!(
                ctx.listen_tcp(addr).await,
                Err(VertxError::TcpIo {
                    operation: "bind",
                    addr: reported,
                    kind: io::ErrorKind::AddrInUse,
                    ..
                }) if reported == addr
            ));

            let first = ctx.listen_tcp(EPHEMERAL).await?;
            let second = ctx.listen_tcp(EPHEMERAL).await?;
            assert_ne!(first.local_addr(), second.local_addr());
            let addr = first.local_addr();
            assert!(matches!(
                ctx.listen_tcp(addr).await,
                Err(VertxError::InvalidOptions(_))
            ));
            drop(first);
            assert_eq!(ctx.listen_tcp(addr).await?.local_addr(), addr);
            Ok(())
        })
    })
    .await
    .unwrap();
    vx.close().await.unwrap();
}

#[tokio::test]
async fn undeploying_first_member_wakes_it_and_keeps_the_listener() {
    let vx = instance(2);
    let (ready, waiting) = oneshot::channel();
    let (first_id, addr, first) = serve(&vx, EPHEMERAL, |_, mut incoming| async move {
        let mut accept = pin!(incoming.accept());
        confirm_pending(accept.as_mut()).await;
        ready.send(()).unwrap();
        assert!(matches!(
            accept.await,
            Err(VertxError::DeploymentTerminated(_))
        ));
        Ok(())
    })
    .await;
    waiting.await.unwrap();
    let (_, _, second) = serve(&vx, addr, |_, mut incoming| async move {
        let (socket, _) = incoming.accept().await?;
        reply(socket, 2).await;
        Ok(())
    })
    .await;
    first_id.stop().await.unwrap();
    first.await.unwrap();
    assert!(std::net::TcpListener::bind(addr).is_err());
    request(addr, 2).await;
    second.await.unwrap();
    vx.close().await.unwrap();
}

fn retain(incoming: TcpIncoming) {
    RETAINED.with(|slot| assert!(slot.borrow_mut().replace(incoming).is_none()));
}

#[tokio::test]
async fn thread_local_listener_allows_event_loop_to_exit_after_last_owner_drops() {
    let vx = instance(1);
    let (report, exited) = oneshot::channel();
    let (ready, bound) = oneshot::channel();
    let deployment = vx
        .deploy(move || {
            local(move |vertx| async move {
                report_on_thread_exit(report);
                let incoming = vertx.listen_tcp(EPHEMERAL).await?;
                let addr = incoming.local_addr();
                retain(incoming);
                ready.send(addr).unwrap();
                Ok(())
            })
        })
        .await
        .unwrap();
    let addr = bound.await.unwrap();
    drop(vx);

    tokio::time::timeout(TIMEOUT, exited)
        .await
        .expect("thread-local TcpIncoming held the EventLoop alive")
        .unwrap();
    deployment.wait_stopped().await.unwrap();
    drop(std::net::TcpListener::bind(addr).expect("the registered port should be released"));
}

#[tokio::test]
async fn last_undeploy_releases_port_even_with_retained_receiver() {
    let vx = instance(1);
    let (ready, bound) = oneshot::channel();
    let id = vx
        .deploy(move || {
            local(move |ctx| async move {
                let incoming = ctx.listen_tcp(EPHEMERAL).await?;
                let addr = incoming.local_addr();
                retain(incoming);
                ready.send(addr).unwrap();
                Ok(())
            })
        })
        .await
        .unwrap();
    let addr = bound.await.unwrap();
    id.stop().await.unwrap();
    let rebound = std::net::TcpListener::bind(addr).unwrap();
    drop(rebound);

    vx.deploy(move || {
        local(move |ctx| async move {
            let replacement = ctx.listen_tcp(addr).await?;
            let mut stale = RETAINED.take().unwrap();
            assert!(matches!(
                stale.accept().await,
                Err(VertxError::DeploymentTerminated(owner)) if owner == id.id()
            ));
            drop(stale);
            assert!(std::net::TcpListener::bind(replacement.local_addr()).is_err());
            Ok(())
        })
    })
    .await
    .unwrap();
    vx.close().await.unwrap();
}

#[tokio::test]
async fn start_panic_releases_listener_even_if_its_handle_is_retained() {
    let vx = instance(1);
    let (ready, bound) = oneshot::channel();
    let result = vx
        .deploy(move || {
            local(move |ctx| async move {
                let listener = ctx.listen_tcp(EPHEMERAL).await?;
                ready.send(listener.local_addr()).unwrap();
                retain(listener);
                panic!("start after binding");
            })
        })
        .await;
    assert!(matches!(result, Err(VertxError::Panic(message)) if message == "start after binding"));
    let addr = bound.await.unwrap();
    let rebound = std::net::TcpListener::bind(addr).unwrap();
    drop(rebound);
    vx.close().await.unwrap();
}

#[tokio::test]
async fn failed_start_rolls_back_listener_before_returning() {
    let vx = instance(1);
    let (ready, bound) = oneshot::channel();
    let result = vx
        .deploy(move || {
            local(move |ctx| async move {
                let incoming = ctx.listen_tcp(EPHEMERAL).await?;
                let addr = incoming.local_addr();
                retain(incoming);
                ready.send(addr).unwrap();
                Err(VertxError::StartFailed(
                    "injected TCP startup failure".into(),
                ))
            })
        })
        .await;
    assert!(matches!(result, Err(VertxError::StartFailed(_))));
    let addr = bound.await.unwrap();
    let rebound = std::net::TcpListener::bind(addr).unwrap();
    drop(rebound);
    vx.close().await.unwrap();
}

#[tokio::test]
async fn cancelling_deploy_rolls_back_its_registration() {
    let vx = instance(1);
    let owner = vx.clone();
    let (ready, bound) = oneshot::channel();
    let deploy = tokio::spawn(async move {
        owner
            .deploy(move || {
                local(move |ctx| async move {
                    let incoming = ctx.listen_tcp(EPHEMERAL).await?;
                    let addr = incoming.local_addr();
                    retain(incoming);
                    ready
                        .send((ctx.current_deployment().unwrap(), addr))
                        .unwrap();
                    pending::<Result<()>>().await
                })
            })
            .await
    });
    let (id, addr) = bound.await.unwrap();
    deploy.abort();
    assert!(deploy.await.unwrap_err().is_cancelled());
    id.wait_stopped().await.unwrap();
    let rebound = std::net::TcpListener::bind(addr).unwrap();
    drop(rebound);
    vx.close().await.unwrap();
}

#[tokio::test]
async fn registration_racing_with_close_leaves_no_bound_port() {
    let vx = instance(2);
    let (ctx, _) = super::support::capture(&vx).await;
    let barrier = std::sync::Arc::new(Barrier::new(2));
    let (reported, observed) = oneshot::channel();
    let vertx = vx.handle();
    let task = ctx
        .spawn({
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                match vertx.listen_tcp(EPHEMERAL).await {
                    Ok(incoming) => {
                        let addr = incoming.local_addr();
                        retain(incoming);
                        reported.send(Some(addr)).unwrap();
                    }
                    Err(VertxError::VertxClosed | VertxError::DeploymentTerminated(_)) => {
                        reported.send(None).unwrap();
                    }
                    Err(error) => return Err(error),
                }
                Ok(())
            }
        })
        .unwrap();
    barrier.wait().await;
    vx.close().await.unwrap();
    task.await.unwrap();
    if let Some(addr) = observed.await.unwrap() {
        let rebound = std::net::TcpListener::bind(addr).unwrap();
        drop(rebound);
    }
}

#[tokio::test]
async fn close_wakes_accept_and_drops_socket_owned_by_managed_task() {
    struct OnDrop(Option<oneshot::Sender<()>>);
    impl Drop for OnDrop {
        fn drop(&mut self) {
            let _ = self.0.take().unwrap().send(());
        }
    }

    let vx = Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .shutdown_grace(Duration::from_millis(500))
        .build()
        .unwrap();
    let (ready, waiting) = oneshot::channel();
    let (started, holding) = oneshot::channel();
    let (dropped, mut drop_notice) = oneshot::channel();
    let (_, addr, acceptor) = serve(&vx, EPHEMERAL, |ctx, mut incoming| async move {
        let (socket, _) = incoming.accept().await?;
        ctx.spawn_local(async move {
            let _socket = socket;
            let _probe = OnDrop(Some(dropped));
            started.send(()).unwrap();
            pending::<Result<()>>().await
        })?;

        let mut accept = pin!(incoming.accept());
        confirm_pending(accept.as_mut()).await;
        ready.send(()).unwrap();
        assert!(matches!(accept.await, Err(VertxError::VertxClosed)));
        Ok(())
    })
    .await;
    let mut client = TcpStream::connect(addr).await.unwrap();
    holding.await.unwrap();
    waiting.await.unwrap();
    assert!(drop_notice.try_recv().is_err());
    vx.close().await.unwrap();
    acceptor.await.unwrap();
    drop_notice.try_recv().unwrap();
    assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
}
