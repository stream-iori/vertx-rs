# 生命周期与关闭

[返回文档索引](index.md)

```rust
use std::future::{Future, ready};
use vertx_rs::{Result, VertxHandle};

pub trait Verticle: 'static {
    fn start(&mut self, vertx: VertxHandle) -> impl Future<Output = Result<()>>;
    fn stop(&mut self, _vertx: VertxHandle) -> impl Future<Output = Result<()>> {
        ready(Ok(()))
    }
}
```

```text
Starting → start 成功 → Running → 请求停止 → Stopping → Stopped
    └─ 失败或取消 ────────────────────────────┘
```

- `Vertx::deploy(|| MyVerticle::new()).await` 把 Send factory 送到目标 EventLoop，在那里创建 Verticle。Verticle 及其生命周期 Future 可以持有 `Rc`。用 `deploy_worker` 部署 Worker 时，两者都要求 `Send`。
- 部署在提交到执行器之前登记。成功返回的 `Deployment` 表示异步 `start` 已完成，消息循环可以用 `vertx.spawn_local` 或 EventBus handler 继续运行。
- 丢弃部署等待 Future 会请求停止，后台销毁用户状态和注销资源。start 返回错误或被取消时仍尝试 stop。factory 或 start panic 时框架继续清理，跳过用户 stop。
- 生命周期隔离回调构造、每次 poll 及析构时的 panic。stop 的这些阶段都恢复清理用 Context，保留首次错误及后续清理错误。
- `deploy_with` 接收 `Fn(Arc<C>) -> V`，成功返回 `Vec<Deployment>`。中途失败时先请求停止本批已经启动的部署，再依次等待。返回的 `DeploymentFailed` 保留启动错误和回滚错误。取消等待时同时清理已启动和仍在启动的部署。

## 受管任务

| 入口 | 约束 |
|---|---|
| `Vertx::spawn` / `VertxHandle::spawn` | Send Future 和结果。在本实例的受管执行中继承当前目标，框架外派发到实例的 EventLoop |
| `spawn_local` | 在本实例当前 EventLoop 上提交。Future 和结果可以持有 `Rc` |
| `Deployment::spawn` | 从任意线程定向提交 Send Future，到指定部署的 EventLoop 或 Worker |
| `execute_blocking` / `execute_blocking_unordered` | 共用固定 Worker 池。有序入口按来源 Context 排队，独立的外部调用各有自己的来源 |
| `stopping()` | 返回可移入任务的等待信号。调用时选定当前部署或实例作用域 |

`TaskHandle<T>` 等待得到 `Result<T>`。丢弃句柄会分离等待者，任务继续由作用域管理。
调用 `abort()` 或共享 `abort_handle()` 可以请求中止，等待结果发生在用户 Future 析构之后。
任务提交时分配一次 `TaskId`，诊断和执行器使用同一个 ID。提交失败释放登记，关闭期间迟到的取消句柄会收到已请求的中止。
普通任务错误报告给异常 hook，Consumer 按消息处理错误及 panic，并继续接收。

## 停止部署

`deployment.request_stop()` 幂等发起停止。`stop().await` 发起停止并按 `shutdown_grace` 等待，默认 1 秒。
超时可用 `wait_stopped().await` 接着等。两种等待均返回最终清理结果，部署从登记表移除之后仍可查询。
当前部署需要停止自身时，用 `vertx.current_deployment().unwrap().request_stop()`。等待自身返回 `WouldDeadlock`。

1. 封住部署准入。取消停止信号，取出订阅与监听登记，在锁外逐项注销。
2. 给子任务半个关闭宽限期协作退出，再中止剩余异步任务，等待用户 Future 析构。
3. 等待已经开始的 blocking，执行用户 stop。只有实际 stop 回调中创建并首次 poll 的 blocking Future 能提交清理操作。
4. 析构 Verticle，等待清理期间提交的 blocking，封存并析构异常 hook，移除部署记录并发布结果。封存后设置或清除部署 hook 会返回 `DeploymentTerminated`。

中止只能在执行器取得控制权后生效，已开始的 blocking 会自然结束。丢弃 `TcpIncoming` 会注销监听。最后一个登记退出时释放端口。
已接收的 socket 由调用者持有，可交给受管任务随任务清理。

## 关闭实例与服务

- `TcpServer::request_shutdown(grace)` 通知服务停止接收、排空连接，调用后立即返回。服务可以在所属 Context 中发起关闭。首个有效请求确定期限，零宽限期会立即请求中止剩余连接。
- 从外部或其他 Context 调用 `wait_closed()`，可以等到监听登记和连接清理完成。`shutdown(grace)` 同时发起关闭并等待到同一个期限，超时返回 `ShutdownTimeout`。清理继续在所属 EventLoop 执行，仍可用 `wait_closed()` 等结果。在所属 Context 中等待任一接口都会返回 `WouldDeadlock`。
- 如需在途 HTTP 请求继续访问 EventBus 等应用服务，先等待各服务排空，再调用 `Vertx::close()`。直接停止部署或实例时，服务走受管任务取消流程。启动等待被取消时，尚未交付的服务会撤销监听。
- `Vertx::request_close()` 和 `VertxHandle::request_close()` 同步封住准入、发出取消并提交关闭任务。实例内的 EventLoop 和 Worker 都可以调用，重复请求共享一次关闭流程。弱句柄的实例已释放时返回 `VertxClosed`。
- `Vertx::wait_closed().await` 等待最终结果，可以先开始等待，再由其他任务请求关闭。EventLoop 在清理期间继续驱动任务、I/O 和停止回调。所有 Worker 和 EventLoop 完成 join 后才返回成功。
- `Vertx::close().await` 发起关闭并按 `shutdown_grace` 等待。超时返回 `ShutdownTimeout`，丢弃等待 Future 也只结束本次等待，后续可用 `wait_closed()` 读取最终结果。清理错误用 `CleanupFailed` 保留，协调任务的派发错误直接记录和返回。
- 在本实例的受管执行或任一 EventLoop OS 线程上，`close()` 和 `wait_closed()` 都返回 `WouldDeadlock`。此时可以用 `request_close()` 发起关闭。其他实例可以异步等待。
- `is_admission_closed()` 在封住准入时变为 true。需要确认线程退出时显式等待 `wait_closed()` 或 `close()`。释放最后一个 `Vertx` 会发起尽力清理。

[执行模型](execution-model.md) · [错误与诊断](diagnostics.md)
