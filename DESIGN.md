# vertx-rs 0.3 设计与迁移说明

目标：用 Rust Future、固定 OS Worker 线程池和 Tokio IO 驱动实现 Vert.x 5 的**单进程执行核心**。
范围包括 Verticle、Context、Worker、EventBus、Timer、部署及异步关闭。

## 1. 执行保证

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

### 1.1 Context 隔离

`ContextFuture` 在**每次 poll** 前设置 CURRENT，返回 Pending/Ready 或 panic 时立即恢复。
ContextGuard 不可 Send，也不跨 await 保存。Future 取消时，在所属执行环境中设置 Context 后析构用户状态。
Worker 的 Context 就绪队列每次派发一个 poll；Pending 后释放执行权，析构也与普通 poll 互斥。
同 Context 的其他任务可以在该 Future 等待期间执行；同消费者仍等待当前消息 Future 完成再取下一条。

```text
poll(A): CURRENT=A → poll user A → restore
poll(B): CURRENT=B → poll user B → restore
poll(A): CURRENT=A → poll user A → restore
```

Context 与物理线程身份分开记录。调用 execute_blocking 时：

| 查询 | EventLoop 内 | 其 blocking 闭包内 | await 返回后 |
|---|---|---|---|
| Context::current() | 来源 Context | 同一来源 Context | 同一来源 Context |
| ctx.is_event_loop() | true | true | true |
| is_on_event_loop_thread() | true | false | true |
| is_on_worker_thread() | false | true | false |

EventLoop 初始化记录实例 ID、EventLoop ID 和实际 ThreadId，受管 poll 校验物理位置。
Worker 原生线程启动/退出时设置和清除物理线程类型。

### 1.2 EventLoop 直接派发

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
- 普通依赖不启用 Tokio rt-multi-thread；tokio-util 继续用于 CancellationToken、TaskTracker 等工具。
- 本轮只验证派发结构与执行语义，不提供未经测量的性能提升数字。

## 2. 生命周期

```rust
#[async_trait::async_trait(?Send)]
pub trait Verticle: 'static {
    async fn start(&mut self, ctx: Context) -> Result<()>;
    async fn stop(&mut self, ctx: Context) -> Result<()> { Ok(()) }
}
// WorkerVerticle: Send，使用 #[async_trait::async_trait]
```

```text
Starting → start 成功 → Running → 请求撤销 → Stopping → Stopped
    └─ 失败/取消 ──────────────────────────┘
```

- deploy 的 Send factory 在目标 EventLoop 上创建实例；实例本身不必 Send。
- deploy_worker 同样接收 factory，worker 实例和异步生命周期要求 Send。
- **deploy 返回成功表示异步 start 已完成**。start 返回后，部署继续存活，等待撤销。
- 主消息循环通过 Context 启动受管任务。一个任务自然结束不会自动撤销整个部署。
- 部署记录在启动之前登记；取消 deploy Future 会触发撤销，后台继续清理。
- start 返回 Err 时仍尝试 stop；factory/start panic 时跳过用户 stop，执行框架清理。
- stop 错误及 panic 会被返回/报告，资源清理仍继续。每个实例最多执行一次 stop。
- deploy_with 保留 Vec<DeploymentId> 返回形态；中途失败或调用被取消，撤销本批先前实例。
- typed config 使用 Arc<C> 传给各实例 factory。

### 2.1 受管任务

| 接口 | 约束 |
|---|---|
| spawn_local(future) → Result<TaskHandle<()>> | 当前 Context 必须与目标一致，且位于所属 EventLoop；支持 !Send |
| spawn(future) → Result<TaskHandle<()>> | Future 为 Send；派发至目标 Context，EventLoop 目标仍固定线程 |
| run_on_context(future) → Result<TaskHandle<()>> | Send Future，异步入队；外部线程也能调用 |
| execute_blocking(f) | 默认 ordered=true |
| execute_blocking_with(f, ordered) | false 允许同 Context 多个阻塞操作并行 |
| request_undeploy() | 不等待自身结束，适合在本部署任务内发起撤销 |

TaskHandle<T> 的 await 输出为 Result<T>，无需嵌套 await??。
提供 abort、abort_handle、is_finished；TaskAbortHandle 可克隆。
丢弃 TaskHandle 只分离调用方，任务仍由部署管理；取消返回 TaskCancelled，不作为业务异常报告。
is_finished 和等待结果都晚于用户 Future 析构。保留取消句柄或迟到的 Waker 不阻止线程池关闭。
Tracker token 在用户 Future 及其状态析构完成后才释放，覆盖未首次 poll 就被 abort 的情况。
普通任务错误报告给异常 handler；Consumer 按消息捕获错误及 panic，随后继续接收。

### 2.2 撤销与关闭

1. 关闭部署准入，拒绝新任务、订阅、定时器及普通阻塞操作。
2. 注销订阅并取消 scope token；Consumer 不再读取缓冲消息。
3. 给子任务半个 shutdown_grace 协作退出，随后 abort 剩余异步任务，并等待实际析构完成。
4. 等待已经开始的 blocking 操作结束，执行 stop。stop 中可以执行清理所需的 blocking 操作。
5. 等待清理期间产生的 blocking 操作结束，完成部署并释放记录。

undeploy 调用使用一个 shutdown_grace deadline 覆盖上述过程，默认 1 秒。
超时返回 ShutdownTimeout，后台收尾继续。**abort 不能抢占不让出执行权的同步代码，也不能中断已开始的 blocking 闭包。**

close 是幂等异步操作。协调任务在 EventLoop 上执行：关闭全局准入、消费者和定时器，撤销全部部署，
等待 blocking 操作，排空 Worker 任务并等待固定线程退出、join，再关闭 EventLoop 派发准入并请求停止。
业务清理期间 EventLoop 继续驱动命令、wake、Timer 和取消；入队检查与关闭派发准入使用同一个锁。
EventLoop 退出时在所属线程取消剩余命令、销毁 LocalSet 和 runtime，最后清除物理线程身份。
关闭末尾最多创建一个临时回收线程，join 全部 EventLoop 后才发布最终结果；运行期间没有新增常驻回收线程。
Worker 与 EventLoop 都不等待回收自身；join 或回收线程创建失败返回 CleanupFailed，不能误报成功。
显式 close 成功表示所有 Worker 和 EventLoop OS 线程已退出并完成 join。
排空时拒绝新业务，但继续接受已有任务的必要 wake 和取消调度。
取消 close 的等待 Future 不会取消后台清理；超时后实例仍拒绝新工作，可再次等待 close。
stop 失败通过 CleanupFailed 保留；初始化被取消本身不算关闭失败。
同部署任务调用等待自身的 undeploy，或本实例 Context 内调用 close，返回 WouldDeadlock。
本实例任一 EventLoop OS 线程上等待 close 也返回 WouldDeadlock，包括 current() 为 None 的裸 Tokio 任务；
从其他实例的 EventLoop 等待本实例关闭则允许。
Drop 仅发起尽力清理；有确定关闭需求时必须 await close。

## 3. EventBus

```text
address → Route { TypeId, subscribers, round-robin cursor }
                       ↓
             每消费者 mpsc 队列，容量 256
                       ↓
                 Envelope<T>
                       ↓ 可选
             oneshot<Result<ErasedEnvelope>>
```

| API | 行为 |
|---|---|
| consumer<T> | 同地址存活消费者类型必须一致；冲突在注册时返回 TypeMismatch |
| send / send_with | 轮询选择一个消费者；等待该队列空位；成功表示入队 |
| try_send | 不等待；区分 QueueFull、ChannelClosed、NoConsumer |
| publish | 复制给所有消费者；满队列丢弃该次投递并记录丢弃数 |
| request / request_with | 返回响应体；默认 30 秒；一个 deadline 覆盖入队和回复 |
| request_message / request_message_with | 返回完整响应 Envelope，可读取响应 headers |
| reply / reply_with | 一次性响应，后者支持响应 headers |
| fail(code, message) | 返回 RecipientFailure |
| into_parts | 拆出 body、Headers 和可跨任务移动的 ReplyHandle |

请求取消/超时不会撤回已经入队的业务消息；迟到 reply 返回 ReplyClosed。
Headers 按插入顺序保存、键大小写敏感、支持重复值；header 追加，insert 替换，get_all 读取全部值。

### 3.1 归属与竞态

- Context::event_bus 注册始终绑定该 Context。
- Vertx::event_bus 在框架外注册为 unbound；在本实例的 Context 中注册时自动绑定当前 Context。
- 外部 unbound Consumer 使用 recv；需要 handler 时在所属 Context 内创建 Consumer。
- handler 接受本地 !Send 闭包；handler_worker 要求 Send 闭包及 Future。都校验调用 Context。
- scope 存活检查、路由注册、作用域登记持有同一个准入锁；Stopping 不能插入新的登记。
- 路由注销使用 DashMap entry 原子更新，删除最后一个订阅时同时删除轮询状态。
- Consumer Drop 同时撤销路由和 scope 登记，避免长生命周期部署积累已销毁记录。
- 所有同步锁在 await 或用户回调前释放。

## 4. Timer 与配置

Timer 创建时记录绝对 deadline，首次 poll 再晚也不会重新计算延迟。
Periodic 创建时同样记录起点，首次 tick 才初始化 Tokio interval；错过 tick 使用 Skip 策略。
取消检查优先于同轮到期，is_cancelled 同时反映显式取消和作用域取消。
完成的一次性 Timer 不因后续关闭变为已取消。
Context Timer 随 scope 取消；全局 Timer 随 close 取消。
创建接口返回 Result；零周期为 InvalidOptions，关闭后创建返回相应生命周期错误。
等待 Timer/tick 需要 Tokio 时间驱动；Timer 对象不会把调用方 Future 派发到其他 Context。

| 配置 | 默认 |
|---|---|
| EventLoop 线程数 | 2 × available_parallelism()，builder 可覆盖 |
| Worker 与 blocking 共用的 OS 线程数 | 20，worker_threads 可覆盖 |
| shutdown_grace | 1 秒 |
| 消费者容量 | 256 |
| 请求 timeout | 30 秒 |

有序 blocking 使用每 Context 的公平锁保持 FIFO，跟踪记录覆盖排队、实际执行及调用方取消。
普通 Context 执行门不用于 blocking 闭包。等待有序锁或尚未开始的操作在作用域关闭后不再执行用户闭包，已开始执行的操作必须自然结束。
阻塞任务提交后，等待方取消不撤回操作；跟踪记录保持到实际闭包结束。
池内线程被同步操作占满时，其他 Worker 工作等待；关闭超时检测在 EventLoop/调用方上继续工作。

## 5. 错误与可观测性

异常路由：Context exception handler → 全局 exception handler → tracing。
调用前复制 handler 并释放读锁，handler 可重设或清除自身；异常 handler panic 被捕获后写日志，避免递归。
生命周期结束和全局关闭清除相应 handler 引用。
tracing 包含部署 ID、Context ID、EventLoop ID、ThreadId，以及广播丢弃、关闭超时等信息。

结构化错误覆盖：类型冲突、无消费者、队列满、通道关闭、请求超时、回复关闭、业务失败、Context 不匹配、
部署终止、全局关闭、启动失败、panic、关闭超时、多实例回滚失败及整体关闭失败。

## 6. 从 0.2 迁移

| 0.2 | 0.3 |
|---|---|
| run(self, ctx) | start(&mut self, ctx) 与可选 stop(&mut self, ctx) |
| run 内长循环 | 在 start 中 spawn_local/spawn 受管循环，或注册 handler |
| deploy 返回 factory 完成 | deploy 等待异步 start 完成 |
| deploy_worker(instance) | deploy_worker(factory) |
| spawn_local 的输出为 () | Future 输出 Result<()>，调度返回 Result<TaskHandle<()>> |
| handler 无返回值 | handler/handler_worker 返回 Result<()>，使用 ? 或处理错误 |
| timer/periodic 直接返回对象 | 创建时使用 ? 或 unwrap |
| HashMap headers | Headers；with_headers 仍接受 HashMap 转换 |
| execute_blocking 无序 | 默认有序；需要并行时 execute_blocking_with(f, false) |
| 依赖 Drop 结束 | 显式 close().await |

多 handler 共享状态优先级：单 handler 独占 → Rc<Cell> → 小作用域 Rc<RefCell> → 通过内部地址协调。
若 stop 需要访问任务使用的状态，通过这些明确的所有权结构共享；不要跨 await 保存 RefCell guard。

可运行示例仍为 ping_pong、pipeline、verticle_to_verticle，均使用 start/stop 和显式 close。

## 7. 与 Vert.x 5 的对照与范围

| 能力 | 对齐情况 / Rust 选择 |
|---|---|
| EventLoop 线程亲和性 | 对齐；支持 !Send 状态 |
| Context 传播与线程类型区分 | 对齐受管执行入口 |
| Worker 普通执行串行 | 对齐；允许线程迁移 |
| start/stop 就绪与关闭 | 异步钩子；框架增加受管任务取消及明确的等待超时 |
| executeBlocking ordered | 默认有序；与 Worker Future 共用固定 OS 线程池 |
| send/publish/request/reply/fail | 提供；send 的有界背压和 pull Consumer 是 Rust 扩展 |
| Consumer 注册时点 | consumer 调用立即注册，支持 recv；与 Java 在 handler 注册后生效有差异 |
| Timer / Periodic | Future / tick 拉模型；保留创建时间起点与取消语义 |
| 多实例 | 返回 Vec<DeploymentId>，失败时回滚本批 |
| Future/Promise | Rust Future、Result、oneshot；不重复实现 Java 类型 |
| 任意成功的 Future 组合 | futures::future::select_ok；tokio::select! 等待先完成，不能等同 Future.any |
| 配置 | factory 接收 Arc<C> 强类型配置；不提供动态 Context KV |
| 外部 Context | current() 返回 None，不自动创建外部 Context |
| SharedData | 使用 std/Tokio 共享原语；不提供框架门面 |

本轮不包含 HTTP/TCP/WebSocket/FileSystem/DNS/通用流、集群 EventBus、Codec、命名 WorkerExecutor、
Java 虚拟线程、动态部署、旧式按 ID 取消 Timer 和 metrics 服务端点。

官方语义依据：[Context](https://vertx.io/docs/apidocs/io/vertx/core/Context.html)、
[VerticleBase](https://vertx.io/docs/apidocs/io/vertx/core/VerticleBase.html)、
[Message](https://vertx.io/docs/apidocs/io/vertx/core/eventbus/Message.html)、
[Vertx.close](https://vertx.io/docs/apidocs/io/vertx/core/Vertx.html)。

## 8. 原生 Worker 调度与 Future 组合

```text
Future wake → 合并重复通知 → 所属 Context 就绪队列
                                ↓
                         共享 FIFO 就绪队列
                                ↓
                   空闲 OS Worker 执行一次 poll
                     ├─ Pending：释放线程与 Context
                     └─ Ready / abort：析构后通知完成
```

- 每个 Context 同时只有一个队列执行权，每次 poll 后重新排到共享队尾，避免单个 Context 连续独占。
- poll 中发生 wake，记录为再次就绪；已入队任务不重复入队，完成后的迟到 wake 被忽略。
- 执行器持有全部存活任务，包括 Pending 状态；取消在所属 Worker 的 Context 执行权下完成析构。
- Future 和 blocking 共用线程；blocking 有序锁独立于普通 Context 执行权，单线程池内 await execute_blocking 不占住线程。
- Worker 每次 poll 进入分配的 EventLoop runtime Handle，Tokio 仅提供 IO/Timer 注册和驱动。
  Handle::enter 不推进 Future，也不创建 Worker Tokio runtime。实际 Future 调度由原生池完成。
- 原生线程池只提供普通 Future 执行环境，不提供 Tokio 多线程任务环境特有的 block_in_place 等保证。
  裸 tokio::spawn 使用所进入的 Tokio runtime，既不继承 Context，也不成为原生 Worker 任务；使用 Context::spawn 管理业务任务。
- 内部就绪队列不额外设置总容量；每个任务最多保留一个就绪通知。EventBus 的消费者队列仍为 256。
  这不是全局任务数量上限，不承诺更高吞吐量。

直接使用已有 futures 库，不定义 Java 风格 Future/Promise 门面：

| 需求 | Rust API | 结束条件 |
|---|---|---|
| 全成功 | try_join! / try_join_all | 全成功，或遇错提前返回 |
| 全结束并收集结果 | join! / join_all | 全部结束，保留逐项 Result |
| 任意成功 | select_ok | 首次成功；只有全失败才失败 |
| 任意最先完成 | select! / select_all | 首次结束，无论成功或失败 |

普通 Rust Future 是惰性的，必须被 await/poll 才会推进；组合器不自动创建线程，也不改变当前 Context。
组合器提前返回并丢弃剩余普通 Future，会停止继续 poll；已提交的外部 IO 不保证被撤销。
已 spawn 的任务独立运行，丢弃其 TaskHandle 仅分离等待者；需要取消时显式 abort，或撤销所属部署。
select_ok 返回的剩余 Future 由调用方决定继续等待或丢弃。空集合及 Unpin 等约束遵循 futures 原始 API。

本轮不提供旧 API 兼容层：删除 blocking_threads / blocking_thread_count，统一由 worker_threads 配置线程数；
spawn/spawn_local 返回 TaskHandle<()>，原 handle.await?? 改为 handle.await?；
run_on_context 接收 Future，原同步闭包改成 async { ...; Ok(()) }。

## 9. 验收

- 同一个 EventLoop 上多 Context 交替 await 不串线；跨 OS 线程 poll 后 TLS 不残留。
- 普通 Verticle 的 factory/start/handler/受管任务/stop/析构始终位于所属 OS 线程与 Context。
- Worker 同 Context 最大普通并行 poll 数为 1，不同 Context 可以并行。
- blocking 的逻辑 Context 和物理 Worker 身份正确；有序 FIFO、无序并发由共用线程数限制。
- Worker 与 blocking 使用同一组实际 ThreadId；Pending 让出线程、跨线程恢复及 wake 合并不丢通知。
- Worker 内真实异步 IO、Timer、timeout 可用；取消析构和析构 panic 不破坏线程池或 Context。
- EventLoop 同线程派发不内联；跨线程早到取消、运行中取消及关闭入队竞争不遗失任务结果。
- 初始化部分失败回收已创建线程；close 成功时全部 EventLoop 已 join，保留 Context、任务句柄或 Waker 不阻止退出。
- start/stop 错误、panic、取消、关闭、批量回滚和自身撤销均有回归覆盖。
- undeploy 成功返回前受管用户状态已析构；不可中断工作超时返回错误，随后可继续关闭。
- 路由注册/清扫/最后订阅注销竞争不残留路由；请求超时包含满队列等待。
- 业务失败、重复 headers、完整响应、迟到回复、广播满队列行为明确。
- Timer 延后首次 poll、scope/global 取消、周期 Skip 均有测试。
- cargo test、cargo clippy --all-targets -- -D warnings、三个示例运行及格式检查通过。
- 不以未经测量的性能或完整 Vert.x Core 兼容率作承诺。
