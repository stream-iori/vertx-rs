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
use vertx_rs::{
    Context, DeploymentId, Result, TcpConnection, TcpServer, TcpServerOptions, Vertx, VertxError,
};

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
}

async fn start<F, H>(
    vx: &Vertx,
    addr: SocketAddr,
    opts: TcpServerOptions,
    factory: F,
) -> Result<(DeploymentId, TcpServer, Context)>
where
    F: FnOnce() -> H + Send + 'static,
    H: for<'a> AsyncFn(TcpConnection<'a>) -> Result<()> + 'static,
{
    let (tx, rx) = oneshot::channel();
    let id = vx
        .deploy(move |_| {
            local(move |ctx| async move {
                let server = ctx.serve_tcp(addr, opts, factory()).await?;
                tx.send((server, ctx)).map_err(|_| VertxError::ReplyClosed)
            })
        })
        .await?;
    let (server, ctx) = rx.await.map_err(|_| VertxError::ReplyClosed)?;
    Ok((id, server, ctx))
}

#[tokio::test]
async fn one_capacity_budget_covers_all_registrations_and_replacement() {
    let vx = instance();
    let (entered, mut connections) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    let (_, first, _) = start(&vx, ephemeral(), options(1), {
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
    let (_, second, _) = start(&vx, addr, options(1), {
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
    let (_, third, _) = start(&vx, addr, options(1), move || {
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
    let (_, server, ctx) = start(&vx, ephemeral(), options(1), {
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
    ctx.run_on_context(async move {
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

    let (id, server, ctx) = start(&vx, addr, opts, || async |_: TcpConnection<'_>| Ok(()))
        .await
        .unwrap();
    ctx.run_on_context(async move {
        let ctx = Context::current().unwrap();
        assert!(matches!(
            ctx.listen_tcp(addr).await,
            Err(VertxError::InvalidOptions(_))
        ));
        Ok(())
    })
    .unwrap()
    .await
    .unwrap();
    vx.undeploy(id).await.unwrap();
    server.wait_closed().await.unwrap();
    let rebound = std::net::TcpListener::bind(addr).unwrap();
    drop(rebound);
    vx.close().await.unwrap();
}
