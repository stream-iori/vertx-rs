# 执行模型与 Context

[返回文档索引](index.md)

| 执行对象 | 线程保证 | 状态约束 |
|---|---|---|
| 普通 Verticle 的 factory/start/stop/析构 | 始终在部署时分配的同一个 OS 线程 | 不要求 Send，可持有 Rc/Cell/RefCell |
| EventLoop Context 的受管任务和 handler | 同一 EventLoop；await 后仍在原线程 | 支持 !Send Future |
| Worker Context 的普通执行 | 可以换线程；同 Context 的 poll 和任务析构串行 | Send |
| execute_blocking | 与 Worker Future 共用固定线程池；保留来源逻辑 Context | 闭包和结果要求 Send |
| 裸 tokio::spawn/spawn_local | 遵循 Tokio 自身规则，不继承逻辑 Context | 不受部署任务管理 |

亲和性指 **OS 线程**，不绑定 CPU 核心。线程可以由操作系统调度到不同 CPU。
单线程上的多个 async 任务仍会在 await 处交替执行；RefCell 借用不能跨 await。
跨 Verticle 的默认通信方式是 EventBus。Rust 也允许 Arc、原子变量及锁，框架不会强制禁止共享状态。

```text
Vertx
 ├─ EventLoop #0: OS T0 + current-thread runtime + LocalSet
 │    ├─ Verticle A / Context A
 │    └─ Verticle B / Context B
 ├─ EventLoop #1: OS T1 + current-thread runtime + LocalSet
 │    └─ Verticle C / Context C
 ├─ Worker pool：固定 N 个 OS 线程，Context 就绪队列串行派发 Future poll
 │    └─ execute_blocking 共用该池，默认每来源 Context FIFO
 └─ EventBus：进程内共享地址路由，每个消费者一个有界队列
```

## Context 隔离

`ContextFuture` 在**每次 poll** 前设置 CURRENT，返回 Pending/Ready 或 panic 时立即恢复。
ContextGuard 不可 Send，也不跨 await 保存。Future 取消时，在所属执行环境中设置 Context 后析构用户状态。
Worker 的 Context 就绪队列每次派发一个 poll；Pending 后释放执行权，析构也与普通 poll 互斥。
同 Context 的其他任务可以在该 Future 等待期间执行；同消费者仍等待当前消息 Future 完成再取下一条。

```text
poll(A): CURRENT=A → poll user A → restore
poll(B): CURRENT=B → poll user B → restore
poll(A): CURRENT=A → poll user A → restore
```

Context 与物理线程身份分开记录。`Context::threading_model()` 只返回 `EventLoop` 或 `Worker`；
框架外代码的 `Context::current()` 为 None，不自动创建外部 Context。调用 execute_blocking 时：

| 查询 | EventLoop 内 | 其 blocking 闭包内 | await 返回后 |
|---|---|---|---|
| Context::current() | 来源 Context | 同一来源 Context | 同一来源 Context |
| ctx.is_event_loop() | true | true | true |
| is_on_event_loop_thread() | true | false | true |
| is_on_worker_thread() | false | true | false |

用户 Future 析构仍在 Context 执行门内完成；若析构 panic，先完成诊断并正常释放 Context、
Dispatch、Span 和执行门，再向外层执行器恢复展开，避免用户 panic 使执行门中毒。
执行门中毒视为框架内部故障，不取出中毒锁继续执行。

EventLoop 初始化记录实例 ID、EventLoop ID 和实际 ThreadId，受管 poll 校验物理位置。
Worker 原生线程启动/退出时设置和清除物理线程类型。

## EventLoop 直接派发

EventLoop 的 OS 线程、启动握手及退出回收由项目管理；每个线程保留 Tokio current-thread runtime + LocalSet。
不再使用 LocalPoolHandle 或等待本地 JoinHandle 的 Tokio 代理任务。

```text
跨线程派发 → 目标 EventLoop 命令队列 → 一个本地任务 → 用户 Future
同线程派发 ─────────────────────────→ 一个本地任务 → 用户 Future
```

- 跨线程命令传递 Send factory，!Send Future 在目标线程创建；每个任务只创建一个本地 Tokio 任务。
- 同目标 EventLoop 的 spawn 及 spawn_local 直接创建本地任务，不经过跨线程队列，也不内联执行用户 Future。
- 跨线程 TaskHandle 只使用一个结果 oneshot，取消状态覆盖入队前、入队后及绑定本地 AbortHandle 时的竞争。
- 早到的 abort 不会丢失；取消、未首次 poll 的 Future 析构和 panic 清理都在目标线程完成。
- 命令每处理最多 64 条主动让出执行权。命令队列沿用无界方式，不增加背压 API。
- 创建实例等待全部 EventLoop 就绪，记录实际 ThreadId 和 runtime Handle；部分启动失败时回收已创建线程。
- `VertxBuilder::try_build() -> Result<Vertx>` 在创建线程前校验线程数与 shutdown_grace，零值返回 InvalidOptions。
  线程/runtime 创建失败以 Other 保留阶段与底层错误文本；后续回收失败使用 LifecycleFailed 保留首错与 cleanup。
  EventLoop 部分启动失败、或 Worker 初始化失败，均在返回错误前停止并 join 已创建线程。
  `build()` 委托 try_build，失败时 panic；new/Default 使用默认配置构建实例。
- 普通依赖不启用 Tokio rt-multi-thread；tokio-util 继续用于 CancellationToken、TaskTracker 等工具。
- 本轮只验证派发结构与执行语义，不提供未经测量的性能提升数字。

## 相关主题

[生命周期与关闭](lifecycle.md)、[原生 Worker 调度与 Future 组合](worker-and-futures.md)。
