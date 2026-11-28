//! Measures managed task admission, diagnostic snapshots, and task cleanup.

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, watch};
use vertx_rs::{DiagnosticsOptions, Result, Verticle, Vertx, VertxHandle};

const WARMUPS: usize = 2;
const SAMPLES: usize = 7;
const SNAPSHOTS: u32 = 128;

struct Idle;

impl Verticle for Idle {
    async fn start(&mut self, _: VertxHandle) -> Result<()> {
        Ok(())
    }
}

async fn sample(loops: usize, enabled: bool, tasks: usize) -> [Duration; 3] {
    let vertx = Vertx::builder()
        .event_loop_threads(loops)
        .worker_threads(1)
        .shutdown_grace(Duration::from_secs(10))
        .diagnostics(DiagnosticsOptions {
            enabled,
            ..DiagnosticsOptions::default()
        })
        .build()
        .unwrap();
    let mut deployments = Vec::with_capacity(loops);
    for _ in 0..loops {
        deployments.push(vertx.deploy(|| Idle).await.unwrap());
    }

    let ready = Arc::new(Semaphore::new(0));
    let (release, wait) = watch::channel(false);
    let mut handles = Vec::with_capacity(tasks);
    let started = Instant::now();
    for index in 0..tasks {
        let mut wait = wait.clone();
        let ready = ready.clone();
        handles.push(
            deployments[index % loops]
                .spawn(async move {
                    ready.add_permits(1);
                    wait.wait_for(|released| *released).await.unwrap();
                    Ok(())
                })
                .unwrap(),
        );
    }
    ready
        .acquire_many(tasks.try_into().unwrap())
        .await
        .unwrap()
        .forget();
    let submit = started.elapsed();

    let active = Arc::new(Semaphore::new(0));
    let churn = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut sampling_tasks = Vec::new();
    for ctx in &deployments {
        let active = active.clone();
        let churn = churn.clone();
        sampling_tasks.push(
            ctx.spawn(async move {
                active.add_permits(1);
                while churn.load(std::sync::atomic::Ordering::Relaxed) {
                    tokio::task::yield_now().await;
                }
                Ok(())
            })
            .unwrap(),
        );
    }
    active
        .acquire_many(loops.try_into().unwrap())
        .await
        .unwrap()
        .forget();
    let started = Instant::now();
    for _ in 0..SNAPSHOTS {
        black_box(vertx.diagnostics());
    }
    let snapshot = started.elapsed() / SNAPSHOTS;
    churn.store(false, std::sync::atomic::Ordering::Relaxed);
    for task in sampling_tasks {
        task.await.unwrap();
    }

    let started = Instant::now();
    release.send_replace(true);
    for task in handles {
        task.await.unwrap();
    }
    let cleanup = started.elapsed();

    for id in deployments {
        id.stop().await.unwrap();
    }
    vertx.close().await.unwrap();
    [submit, snapshot, cleanup]
}

fn main() {
    if cfg!(debug_assertions) {
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        println!("loops,diagnostics,tasks,submit_ms,snapshot_us,cleanup_ms");
        for loops in [1, 2] {
            for enabled in [false, true] {
                for tasks in [1_000, 10_000] {
                    let mut measurements = [[Duration::ZERO; SAMPLES]; 3];
                    for round in 0..(WARMUPS + SAMPLES) {
                        let values = tokio::time::timeout(
                            Duration::from_secs(30),
                            sample(loops, enabled, tasks),
                        )
                        .await
                        .unwrap();
                        if round >= WARMUPS {
                            for (results, value) in measurements.iter_mut().zip(values) {
                                results[round - WARMUPS] = value;
                            }
                        }
                    }
                    let [submit, snapshot, cleanup] = measurements.map(|mut values| {
                        values.sort_unstable();
                        values[SAMPLES / 2].as_secs_f64()
                    });
                    println!(
                        "{loops},{enabled},{tasks},{:.3},{:.3},{:.3}",
                        submit * 1e3,
                        snapshot * 1e6,
                        cleanup * 1e3,
                    );
                }
            }
        }
    });
}
