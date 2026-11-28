//! Supported ownership patterns for local and cross-Verticle state.

use super::support::{local, vx};
use rstest::rstest;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use vertx_rs::Vertx;

#[rstest]
#[tokio::test]
async fn rc_refcell_shared_state_across_handlers(vx: Vertx) {
    vx.deploy(|| {
        local(|ctx| async move {
            let values = Rc::new(RefCell::new(Vec::<u32>::new()));
            let a = values.clone();
            ctx.event_bus()
                .unwrap()
                .consumer::<u32>("put")?
                .handler(async move |env| {
                    a.borrow_mut().push(*env.body());
                    env.reply(())
                })?;
            ctx.event_bus()
                .unwrap()
                .consumer::<()>("get")?
                .handler(async move |env| {
                    let n = values.borrow().len();
                    env.reply(n)
                })?;
            Ok(())
        })
    })
    .await
    .unwrap();
    vx.event_bus()
        .unwrap()
        .request::<_, ()>("put", 1u32)
        .await
        .unwrap();
    assert_eq!(
        vx.event_bus()
            .unwrap()
            .request::<_, usize>("get", ())
            .await
            .unwrap(),
        1
    );
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn rc_cell_shared_scalar_across_handlers(vx: Vertx) {
    vx.deploy(|| {
        local(|ctx| async move {
            let value = Rc::new(Cell::new(0u32));
            let a = value.clone();
            ctx.event_bus()
                .unwrap()
                .consumer::<u32>("put")?
                .handler(async move |env| {
                    a.set(*env.body());
                    env.reply(())
                })?;
            ctx.event_bus()
                .unwrap()
                .consumer::<()>("get")?
                .handler(async move |env| {
                    let n = value.get();
                    env.reply(n)
                })?;
            Ok(())
        })
    })
    .await
    .unwrap();
    vx.event_bus()
        .unwrap()
        .request::<_, ()>("put", 42u32)
        .await
        .unwrap();
    assert_eq!(
        vx.event_bus()
            .unwrap()
            .request::<_, u32>("get", ())
            .await
            .unwrap(),
        42
    );
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn arc_atomic_counter_shared_across_verticles(vx: Vertx) {
    let count = Arc::new(AtomicUsize::new(0));
    for _ in 0..3 {
        let count = count.clone();
        vx.deploy(move || {
            local(move |ctx| async move {
                ctx.event_bus()
                    .unwrap()
                    .consumer::<()>("count")?
                    .handler(async move |env| {
                        let n = count.fetch_add(1, Ordering::SeqCst) + 1;
                        env.reply(n)
                    })?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }
    for n in 1..=9 {
        assert_eq!(
            vx.event_bus()
                .unwrap()
                .request::<_, usize>("count", ())
                .await
                .unwrap(),
            n
        );
    }
    vx.close().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn arc_immutable_snapshot_shared_across_verticles(vx: Vertx) {
    let snapshot = Arc::new(vec![1, 2, 3]);
    for _ in 0..2 {
        let snapshot = snapshot.clone();
        vx.deploy(move || {
            local(move |ctx| async move {
                ctx.event_bus()
                    .unwrap()
                    .consumer::<()>("snapshot")?
                    .handler(async move |env| {
                        let values = (*snapshot).clone();
                        env.reply(values)
                    })?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }
    assert_eq!(
        vx.event_bus()
            .unwrap()
            .request::<_, Vec<i32>>("snapshot", ())
            .await
            .unwrap(),
        vec![1, 2, 3]
    );
    vx.close().await.unwrap();
}
