use super::{BoundListener, ListenerRegistration, Member, OpenListener, Slot, TcpListeners};
use crate::deployment::{DeploymentScope, ResourceId, ResourceRegistration};
use crate::{Context, DeploymentId, Result};
use std::collections::HashMap;
use std::future::poll_fn;
use std::io::{Read, Write};
use std::pin::pin;
use std::sync::{Arc, Weak, mpsc};
use std::task::Poll;
use std::thread;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Builder;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

const TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn accepted_socket_uses_the_accepting_runtime_after_the_binding_runtime_exits() {
    let (ready, bound) = mpsc::sync_channel(1);
    let (stop, stopped) = oneshot::channel();
    let source = thread::spawn(move || {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
                ready
                    .send(BoundListener {
                        addr: socket.local_addr().unwrap(),
                        socket: Some(socket),
                        turn: Arc::new(AsyncMutex::new(())),
                        registry: Weak::new(),
                    })
                    .unwrap_or_else(|_| panic!("listener receiver dropped"));
                stopped.await.unwrap();
            });
    });

    let listener = bound.recv_timeout(TIMEOUT).unwrap();
    let mut client = std::net::TcpStream::connect_timeout(&listener.addr, TIMEOUT).unwrap();
    client.set_read_timeout(Some(TIMEOUT)).unwrap();
    client.set_write_timeout(Some(TIMEOUT)).unwrap();
    let target = Builder::new_current_thread().enable_all().build().unwrap();
    let (mut server, _) = target.block_on(async {
        tokio::time::timeout(TIMEOUT, poll_fn(|cx| listener.poll_accept(cx)))
            .await
            .unwrap()
            .unwrap()
    });

    drop(listener);
    stop.send(()).unwrap();
    source.join().unwrap();

    target.block_on(async {
        {
            let mut read = pin!(server.read_u8());
            poll_fn(|cx| {
                assert!(std::future::Future::poll(read.as_mut(), cx).is_pending());
                Poll::Ready(())
            })
            .await;
            client.write_all(&[41]).unwrap();
            assert_eq!(
                tokio::time::timeout(TIMEOUT, read).await.unwrap().unwrap(),
                41
            );
        }
        server.write_u8(42).await.unwrap();
    });
    let mut byte = [0];
    client.read_exact(&mut byte).unwrap();
    assert_eq!(byte, [42]);
}

#[tokio::test]
async fn unregistration_reserves_address_and_tracks_close_until_poll_releases_listener() {
    let registry = TcpListeners::default();
    let scope = DeploymentScope::new(DeploymentId::next(), CancellationToken::new());
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    let bound = Arc::new(BoundListener {
        addr,
        socket: Some(socket),
        turn: Arc::new(AsyncMutex::new(())),
        registry: Arc::downgrade(&registry.state),
    });
    let id = ResourceId::next();
    let registration = ListenerRegistration {
        registry: Arc::downgrade(&registry.state),
        addr,
        id,
        deployment: scope.deployment,
    };
    scope
        .admit()
        .unwrap()
        .register_resource(id, ResourceRegistration::Listener(registration));
    registry.state.lock().unwrap().entries.insert(
        addr,
        Slot::Open(OpenListener {
            bound: bound.clone(),
            members: HashMap::from([(
                scope.deployment,
                Member {
                    id,
                    tracked: scope.task_token(),
                },
            )]),
            budget: None,
        }),
    );

    scope.request_stop();
    scope.unregister_resources();
    let done = match registry.state.lock().unwrap().entries.get(&addr) {
        Some(Slot::Closing { done, .. }) => done.clone(),
        _ => panic!("address should be reserved until the in-flight poll completes"),
    };
    assert!(!done.is_cancelled());
    assert!(std::net::TcpListener::bind(addr).is_err());

    let mut drained = pin!(scope.drain_tasks(TIMEOUT));
    poll_fn(|cx| {
        assert!(std::future::Future::poll(drained.as_mut(), cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(bound);
    assert!(done.is_cancelled());
    drained.await;
    assert!(registry.state.lock().unwrap().entries.is_empty());
    let rebound = std::net::TcpListener::bind(addr).unwrap();
    drop(rebound);
}

#[tokio::test]
async fn stale_registration_from_same_deployment_does_not_close_the_new_listener() {
    struct Rebind;
    impl crate::Verticle for Rebind {
        async fn start(&mut self, ctx: Context) -> Result<()> {
            let old = ctx.listen_tcp("127.0.0.1:0".parse().unwrap()).await?;
            let addr = old.local_addr();
            let registration = old.registration.clone();
            drop(old);
            let mut current = ctx.listen_tcp(addr).await?;
            assert_ne!(current.registration.id, registration.id);
            assert!(registration.unregister().is_none());
            let (client, accepted) = tokio::join!(TcpStream::connect(addr), current.accept());
            client.unwrap();
            accepted.unwrap();
            Ok(())
        }
    }
    let vertx = crate::Vertx::builder()
        .event_loop_threads(1)
        .worker_threads(1)
        .build()
        .unwrap();
    vertx.deploy(|_| Rebind).await.unwrap();
    vertx.close().await.unwrap();
}
