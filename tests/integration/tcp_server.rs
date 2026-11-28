use super::support::local;
use std::future::pending;
use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::ops::AsyncFn;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Semaphore, mpsc, oneshot};
use vertx_rs::{Deployment, Result, TcpConnection, TcpServer, TcpServerOptions, Vertx, VertxError};

const GRACE: Duration = Duration::from_secs(2);
const DEADLINE: Duration = Duration::from_secs(5);

fn ephemeral() -> SocketAddr {
    (Ipv4Addr::LOCALHOST, 0).into()
}

fn options(max: usize) -> TcpServerOptions {
    TcpServerOptions {
        max_connections: NonZeroUsize::new(max).unwrap(),
    }
}

fn instance() -> Vertx {
    Vertx::builder()
        .event_loop_threads(2)
        .worker_threads(1)
        .shutdown_grace(GRACE)
        .build()
        .unwrap()
}

async fn start<F, H>(
    vx: &Vertx,
    addr: SocketAddr,
    opts: TcpServerOptions,
    factory: F,
) -> Result<(Deployment, TcpServer)>
where
    F: FnOnce() -> H + Send + 'static,
    H: for<'a> AsyncFn(TcpConnection<'a>) -> Result<()> + 'static,
{
    let (tx, rx) = oneshot::channel();
    let id = vx
        .deploy(move || {
            local(move |ctx| async move {
                let server = ctx.serve_tcp_local(addr, opts, factory()).await?;
                tx.send(server).map_err(|_| VertxError::ReplyClosed)
            })
        })
        .await?;
    let server = rx.await.map_err(|_| VertxError::ReplyClosed)?;
    Ok((id, server))
}

#[tokio::test]
async fn oversized_connection_limit_is_rejected_before_binding() {
    let vx = instance();
    let occupied = std::net::TcpListener::bind(ephemeral()).unwrap();
    let result = start(
        &vx,
        occupied.local_addr().unwrap(),
        options(Semaphore::MAX_PERMITS + 1),
        || async |_: TcpConnection<'_>| Ok(()),
    )
    .await;
    assert!(matches!(result, Err(VertxError::InvalidOptions(_))));
    vx.close().await.unwrap();
}

#[tokio::test]
async fn one_capacity_budget_covers_all_registrations_and_replacement() {
    let vx = instance();
    let (entered, mut connections) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    let (_, first) = start(&vx, ephemeral(), options(1), {
        let entered = entered.clone();
        let release = release.clone();
        move || {
            let local = Rc::new(());
            async move |connection: TcpConnection<'_>| {
                let _local = &local;
                entered.send(1).unwrap();
                connection.shutdown_token().cancelled().await;
                release.acquire().await.unwrap().forget();
                Ok(())
            }
        }
    })
    .await
    .unwrap();
    let addr = first.local_addr();
    let client = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        tokio::time::timeout(DEADLINE, connections.recv())
            .await
            .unwrap(),
        Some(1)
    );

    let mut closing = std::pin::pin!(first.shutdown(GRACE));
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(closing.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;

    let conflict = start(
        &vx,
        addr,
        options(2),
        || async |_: TcpConnection<'_>| Ok(()),
    )
    .await;
    assert!(matches!(conflict, Err(VertxError::InvalidOptions(_))));
    let (_, second) = start(&vx, addr, options(1), {
        let entered = entered.clone();
        move || {
            async move |mut connection: TcpConnection<'_>| {
                entered.send(2).unwrap();
                connection.write_u8(42).await.unwrap();
                Ok(())
            }
        }
    })
    .await
    .unwrap();
    let (_, third) = start(&vx, addr, options(1), move || {
        async move |mut connection: TcpConnection<'_>| {
            entered.send(3).unwrap();
            connection.write_u8(42).await.unwrap();
            Ok(())
        }
    })
    .await
    .unwrap();
    let mut waiting = TcpStream::connect(addr).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), connections.recv())
            .await
            .is_err()
    );
    release.add_permits(1);
    closing.await.unwrap();
    assert!(matches!(
        tokio::time::timeout(DEADLINE, connections.recv())
            .await
            .unwrap(),
        Some(2 | 3)
    ));
    assert_eq!(waiting.read_u8().await.unwrap(), 42);
    drop(client);
    tokio::try_join!(second.shutdown(GRACE), third.shutdown(GRACE)).unwrap();
    vx.close().await.unwrap();
}

#[tokio::test]
async fn timeout_cancels_connections_even_after_shutdown_waiter_is_dropped() {
    struct Probe(Arc<AtomicBool>);
    impl Drop for Probe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let vx = instance();
    let dropped = Arc::new(AtomicBool::new(false));
    let ready = Arc::new(Semaphore::new(0));
    let (ctx, server) = start(&vx, ephemeral(), options(1), {
        let dropped = dropped.clone();
        let ready = ready.clone();
        move || {
            async move |_: TcpConnection<'_>| {
                let _probe = Probe(dropped.clone());
                ready.add_permits(1);
                pending().await
            }
        }
    })
    .await
    .unwrap();
    let copy = server.clone();
    ctx.spawn(async move {
        assert!(matches!(
            copy.shutdown(GRACE).await,
            Err(VertxError::WouldDeadlock)
        ));
        Ok(())
    })
    .unwrap()
    .await
    .unwrap();
    let mut client = TcpStream::connect(server.local_addr()).await.unwrap();
    tokio::time::timeout(DEADLINE, ready.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let grace = Duration::from_millis(50);
    {
        let mut shutdown = std::pin::pin!(server.shutdown(grace));
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(shutdown.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    }
    server.request_shutdown(GRACE).unwrap();
    tokio::time::timeout(DEADLINE, server.wait_closed())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(server.shutdown(GRACE).await, Err(VertxError::ShutdownTimeout(value)) if value == grace)
    );
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
    vx.close().await.unwrap();
}

#[tokio::test]
async fn owning_context_can_request_shutdown_and_an_external_waiter_observes_drain() {
    let vx = instance();
    let entered = Arc::new(Semaphore::new(0));
    let draining = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let (deployment, server) = start(&vx, ephemeral(), options(1), {
        let entered = entered.clone();
        let draining = draining.clone();
        let release = release.clone();
        move || {
            async move |mut connection: TcpConnection<'_>| {
                entered.add_permits(1);
                connection.shutdown_token().cancelled().await;
                draining.add_permits(1);
                release.acquire().await.unwrap().forget();
                connection.write_u8(42).await.unwrap();
                Ok(())
            }
        }
    })
    .await
    .unwrap();

    assert!(matches!(
        server.request_shutdown(Duration::MAX),
        Err(VertxError::InvalidOptions(_))
    ));
    let mut client = TcpStream::connect(server.local_addr()).await.unwrap();
    tokio::time::timeout(DEADLINE, entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();

    let owned = server.clone();
    deployment
        .spawn(async move {
            owned.request_shutdown(GRACE)?;
            assert!(matches!(
                owned.wait_closed().await,
                Err(VertxError::WouldDeadlock)
            ));
            Ok(())
        })
        .unwrap()
        .await
        .unwrap();
    tokio::time::timeout(DEADLINE, draining.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let closed = server.wait_closed();
    tokio::pin!(closed);
    assert!(futures::poll!(&mut closed).is_pending());

    release.add_permits(1);
    assert_eq!(
        tokio::time::timeout(DEADLINE, client.read_u8())
            .await
            .unwrap()
            .unwrap(),
        42
    );
    tokio::time::timeout(DEADLINE, closed)
        .await
        .unwrap()
        .unwrap();
    vx.close().await.unwrap();
}

#[tokio::test]
async fn external_shutdown_timeout_leaves_connection_abort_on_the_event_loop() {
    let vx = instance();
    let entered = Arc::new(Semaphore::new(0));
    let (deployment, server) = start(&vx, ephemeral(), options(1), {
        let entered = entered.clone();
        move || {
            async move |_: TcpConnection<'_>| {
                entered.add_permits(1);
                pending().await
            }
        }
    })
    .await
    .unwrap();
    let mut client = TcpStream::connect(server.local_addr()).await.unwrap();
    tokio::time::timeout(DEADLINE, entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let (paused, blocking) = oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let owner = deployment
        .spawn(async move {
            paused.send(()).ok();
            resume.recv().ok();
            Ok(())
        })
        .unwrap();
    tokio::time::timeout(DEADLINE, blocking)
        .await
        .unwrap()
        .unwrap();
    let result = tokio::time::timeout(DEADLINE, server.shutdown(Duration::ZERO)).await;
    release.send(()).unwrap();
    assert!(matches!(result.unwrap(), Err(VertxError::ShutdownTimeout(grace)) if grace.is_zero()));
    owner.await.unwrap();

    tokio::time::timeout(DEADLINE, server.wait_closed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        tokio::time::timeout(DEADLINE, client.read(&mut [0]))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    drop(std::net::TcpListener::bind(server.local_addr()).unwrap());
    vx.close().await.unwrap();
}

#[tokio::test]
async fn bind_failure_rolls_back_and_raw_registration_conflicts() {
    let vx = instance();
    let reserve = std::net::TcpListener::bind(ephemeral()).unwrap();
    let addr = reserve.local_addr().unwrap();
    let opts = options(2);
    let result = start(&vx, addr, opts, || async |_: TcpConnection<'_>| Ok(())).await;
    assert!(matches!(
        result,
        Err(VertxError::TcpIo {
            operation: "bind",
            kind: std::io::ErrorKind::AddrInUse,
            ..
        })
    ));
    drop(reserve);

    let (id, server) = start(&vx, addr, opts, || async |_: TcpConnection<'_>| Ok(()))
        .await
        .unwrap();
    let ctx = vx.handle();
    id.spawn(async move {
        assert!(matches!(
            ctx.listen_tcp(addr).await,
            Err(VertxError::InvalidOptions(_))
        ));
        Ok(())
    })
    .unwrap()
    .await
    .unwrap();
    id.stop().await.unwrap();
    server.wait_closed().await.unwrap();
    let rebound = std::net::TcpListener::bind(addr).unwrap();
    drop(rebound);
    vx.close().await.unwrap();
}

#[tokio::test]
async fn external_server_runs_a_local_connection_future_and_closes_with_vertx() {
    let vx = instance();
    let started = vx.serve_tcp(
        ephemeral(),
        options(1),
        async move |mut connection: TcpConnection<'_>| {
            let state = Rc::new(42u8);
            let before = vertx_rs::ExecutionInfo::current().unwrap();
            assert!(before.deployment_id.is_none());
            assert_eq!(connection.read_u8().await.unwrap(), *state);
            assert_eq!(vertx_rs::ExecutionInfo::current(), Some(before));
            connection.write_u8(*state).await.unwrap();
            Ok(())
        },
    );
    let server = tokio::spawn(started).await.unwrap().unwrap();
    let addr = server.local_addr();
    let mut client = TcpStream::connect(addr).await.unwrap();
    client.write_u8(42).await.unwrap();
    assert_eq!(
        tokio::time::timeout(DEADLINE, client.read_u8())
            .await
            .unwrap()
            .unwrap(),
        42
    );

    vx.close().await.unwrap();
    server.wait_closed().await.unwrap();
    drop(std::net::TcpListener::bind(addr).unwrap());
}
