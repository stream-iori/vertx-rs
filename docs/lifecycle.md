# 生命周期与关闭

[返回文档索引](index.md)

```rust
use std::future::{Future, ready};
use vertx_rs::{Context, Result};

pub trait Verticle: 'static {
    fn start(&mut self, ctx: Context) -> impl Future<Output = Result<()>>;
    fn stop(&mut self, _ctx: Context) -> impl Future<Output = Result<()>> {
        ready(Ok(()))
    }
}
// WorkerVerticle 的实例和这两个方法返回的 Future 都要求 Send。
```

```text
Starting → start 成功 → Running → 请求撤销 → Stopping → Stopped
    └─ 失败/取消 ──────────────────────────┘
```

- deploy 的 Send factory 在目标 EventLoop 上创建实例；实例本身不必 Send。
- deploy_worker 同样接收 factory，worker 实例和异步生命周期要求 Send。
- 两种生命周期分别由普通异步函数 drive_local / drive_worker 编排，保留各自 Send/!Send 约束；清理、通知和错误处理复用辅助函数。
- **deploy 返回成功表示异步 start 已完成**。start 返回后，部署继续存活，等待撤销。
- 主消息循环通过 Context 启动受管任务。一个任务自然结束不会自动撤销整个部署。
- 部署记录在启动之前登记；取消 deploy Future 会触发撤销，后台继续清理。
- start 返回 Err 或启动中收到本部署取消时，仍尝试 stop。factory 或 start panic 时跳过用户 stop，继续框架清理。其他部署的终止错误按失败处理。
- 生命周期隔离回调构造 Future、poll 和析构阶段的 panic。stop 的每个阶段都恢复清理用 Context，stop 最多执行一次。清理继续执行并保留首次失败和后续错误。
- `deploy_with` 成功时返回各实例的 ID。中途失败时先向本批所有已启动实例发出停止请求，再按部署顺序等待清理。返回的 `DeploymentFailed` 同时包含启动错误和回滚错误。
- 取消等待会请求停止本批已启动的实例和仍在启动的实例，后台继续清理。
- 各实例的 factory 通过 `Arc<C>` 共用类型化配置。实例数用 `NonZeroUsize` 表示，默认 1。

## 受管任务

| 接口 | 约束 |
|---|---|
| spawn_local(future) → Result<TaskHandle<()>> | 当前 Context 必须与目标一致，且位于所属 EventLoop；支持 !Send |
| spawn(future) → Result<TaskHandle<()>> | 可从外部线程派发 Send Future。EventLoop 目标保持固定线程，始终延后执行 |
| execute_blocking(f) | 同一来源 Context 按 FIFO 派发 |
| execute_blocking_unordered(f) | 同一来源 Context 可以并行执行，受共用 Worker 线程数限制 |
| request_undeploy() | 不等待自身结束，适合在本部署任务内发起撤销 |

TaskHandle<T> 的 await 输出为 Result<T>，无需嵌套 await??。
提供 abort、abort_handle、is_finished；TaskAbortHandle 可克隆。
丢弃 TaskHandle 只分离调用方，任务仍由部署管理；取消返回 TaskCancelled，不作为业务异常报告。
is_finished 和等待结果都晚于用户 Future 析构。保留取消句柄或迟到的 Waker 不阻止线程池关闭。
任务在提交时分配一次 `TaskId`，部署登记、诊断记录、执行器和任务句柄共用这个 ID。
任务在部署准入时登记，再于锁外提交执行器并挂接 abort 句柄。拒绝提交撤销登记，迟到句柄遵循已请求的 abort。
跟踪令牌和任务登记在用户 Future 析构后释放，覆盖未首次 poll 就被 abort 的情况。
普通任务错误报告给异常 handler；Consumer 按消息捕获错误及 panic，随后继续接收。

`Context::accepts_work()` 查询部署此刻是否接收普通工作。提交时仍会重新校验准入。

## 撤销与关闭

1. 关闭部署准入，拒绝新任务、订阅、TCP 监听、定时器及普通阻塞操作。
2. 取消 scope token，取出该部署的订阅和监听登记，再逐项在锁外注销。Consumer 停止读取缓冲消息，等待中的 `accept()` 返回部署终止或实例关闭错误。
3. 给子任务半个 shutdown_grace 协作退出，随后 abort 剩余异步任务，并等待实际析构完成。
4. 等待已经开始的 blocking 操作结束，执行 stop。只有实际 stop 回调可以提交清理所需的 blocking 操作。
5. 等待清理期间产生的 blocking 操作结束，析构异常 hook 并移除部署记录，随后向等待者发布完成结果。

丢弃 `TcpIncoming` 也会注销注册。最后一个注册注销时释放监听 socket，部署的清理完成通知晚于此释放。
已接收的 `TcpStream` 归调用者所有。将 socket 交给 `ctx.spawn_local` 启动的受管任务，任务结束或撤销时，任务独占的连接随之释放。

`TcpServer::shutdown(grace)` 停止单个服务注册的接收，并通知已有连接收尾。首个调用确定期限。
超时会请求中止剩余连接任务，`wait_closed()` 等待实际析构。取消关闭等待或丢弃服务句柄不会改变该服务的生命周期。
持有同一 Context 的任务等待本服务关闭会返回 `WouldDeadlock`。
对于需要 EventBus 的在途 HTTP 请求，从部署外启动同一地址上各个服务的 `shutdown(grace)`，等它们清理后再调用 `Vertx::close()`。
Axum 适配器在这一阶段通知 Hyper 完成在途响应。若某个服务返回 `ShutdownTimeout`，可用该句柄的 `wait_closed()` 继续等连接析构。
直接撤销部署或关闭实例时，服务和连接走普通受管任务取消流程。

undeploy 调用使用一个 shutdown_grace deadline 覆盖上述过程，默认 1 秒。
超时返回 ShutdownTimeout，后台收尾继续。**abort 不能抢占不让出执行权的同步代码，也不能中断已开始的 blocking 闭包。**

close 是幂等异步操作。协调任务在 EventLoop 上执行：关闭全局准入、消费者和定时器，撤销全部部署并释放监听端口，
等待 blocking 操作，排空 Worker 任务并等待固定线程退出、join，再关闭 EventLoop 派发准入并请求停止。
关闭新部署准入与取得当前部署清单是一次状态变更。业务清理期间 EventLoop 继续驱动命令、wake、Timer 和取消。
EventLoop 退出时在所属线程取消剩余命令、销毁 LocalSet 和 runtime，最后清除物理线程身份。
关闭末尾最多创建一个临时回收线程，join 全部 EventLoop 后才发布最终结果。
Worker 与 EventLoop 都不等待回收自身；join 或回收线程创建失败返回 CleanupFailed，不能误报成功。
显式 close 成功表示所有 Worker 和 EventLoop OS 线程已退出并完成 join。
`Vertx::is_admission_closed()` 在关闭准入后即返回 true。后台排空期间，已有任务仍可被唤醒或取消。
取消 close 的等待 Future 不会取消后台清理；超时后实例仍拒绝新工作，可再次等待 close。
stop 及异常 hook 析构失败通过 `CleanupFailed` 保留。仅启动取消时按正常关闭处理。
同部署任务调用等待自身的 undeploy，或本实例 Context 内调用 close，返回 WouldDeadlock。
本实例任一 EventLoop OS 线程上等待 close 也返回 WouldDeadlock，包括 current() 为 None 的裸 Tokio 任务；
从其他实例的 EventLoop 等待本实例关闭则允许。
Drop 仅发起尽力清理；有确定关闭需求时必须 await close。

## 相关主题

[执行模型与 Context](execution-model.md)、[错误与可观测性](diagnostics.md)。
