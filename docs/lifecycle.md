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
4. 析构 Verticle，等待清理期间提交的 blocking，析构异常 hook，移除部署记录并发布结果。

中止只能在执行器取得控制权后生效，已开始的 blocking 会自然结束。丢弃 `TcpIncoming` 会注销监听。最后一个登记退出时释放端口。
已接收的 socket 由调用者持有，可交给受管任务随任务清理。

## 关闭实例与服务

- `TcpServer::shutdown(grace)` 停止该服务接收并通知现有连接排空。首个调用确定期限。超时会请求中止剩余连接，`wait_closed()` 等待清理完成。服务执行环境中等待自己的服务关闭会得到 `WouldDeadlock`。
- 如需在途 HTTP 请求继续访问 EventBus 等应用服务，先等待各服务排空，再调用 `Vertx::close()`。直接停止部署或实例时，服务走受管任务取消流程。启动等待被取消时，尚未交付的服务会撤销监听。
- `Vertx::close()` 幂等关闭实例和部署准入，发出取消，清理实例资源与全部部署，再 join Worker 和 EventLoop。关闭协调期间，EventLoop 继续驱动任务、I/O 和停止回调。线程都已退出并完成 join 后才返回成功。
- `is_admission_closed()` 在封住准入时变为 true。超时或丢弃等待 Future 都只影响本次等待，后续可再次等待 `close()`。关闭错误用 `CleanupFailed` 保留。
- 本实例任一 EventLoop OS 线程或受管执行中等待 `close()` 会得到 `WouldDeadlock`。其他实例的 EventLoop 可以等待。需要确认线程退出时显式等待 `close()`，释放最后一个 `Vertx` 会发起尽力清理。

[执行模型](execution-model.md) · [错误与诊断](diagnostics.md)
