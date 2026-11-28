# 错误与可观测性

[返回文档索引](index.md)

```text
用户 Future / blocking / 消息
       ├─ tracing Span + Dispatch：每次 poll / drop 恢复，不跨 await 持有 guard
       └─ Diagnostics：计数 + 当前线程操作 + 可选的有界任务详情
                            ↑
                  Vertx::diagnostics() 外部直接读取
```

| 入口 / 数据 | 保证 |
|---|---|
| `Context::id()` / `TaskHandle::id()` | 进程内唯一；任务句柄与对应诊断记录使用相同 TaskId |
| `VertxBuilder::diagnostics(DiagnosticsOptions)` | 默认启用轻量计数及线程当前操作；`enabled=false` 关闭诊断记录 |
| `task_details` / `max_task_details` | 详情默认关闭；开启后默认最多 1024 个活跃记录，截断有标记；不保留历史 |
| `queue_details` / `max_queue_details` | 队列详情默认关闭；开启后快照默认最多 1024 条，queue_details_truncated 标记截断 |
| `Vertx::diagnostics()` | 同步采样，不向目标 EventLoop 发命令，不获取 Context 执行锁；不是全局事务快照 |
| 活跃计数 | 部署、任务、就绪任务、路由消费者、blocking 等待/运行；任务包含生命周期驱动及 blocking |
| 累计计数 | 任务成功/失败/取消、请求超时、广播丢弃、队列满事件；用户状态析构后才完成任务记录 |
| 队列摘要 | queued_messages 为已入队元素，reserved_slots 为已获许可，waiting_senders 为等待准入的发送者；详情含地址、消费者/Context/部署 ID 及容量 |
| 线程当前操作 | OS ThreadId、ContextId、TaskId、物理 EventLoopId、操作及开始时间；只列正在执行的受管 poll/drop/blocking |
| 清理进度 | 部署 queued/starting/running/quiescing/stopping；close 的 deployments/blocking/worker_join/event_loop_join/closed/failed |

部署与关闭的诊断阶段在内部使用私有枚举；只在快照与日志输出时转换为既有字符串。
Stopped 阶段按枚举移除部署记录。

诊断只保存 ID、时间和小型元数据，不强持有 Context、Verticle 或用户 Future。
任务销毁时删除详情；晚到的 wake 不复活记录。未采集的任务不会为了补齐列表而被保留。
消费者数量指有效路由订阅；scope 清扫、close 与 Consumer Drop 不重复扣减。
诊断锁内取样计数、阶段和详情元数据。耗时换算、队列详情复制和排序在锁外完成。
裸 Tokio 任务、用户自建线程及等待 Worker 执行门的时间不在监控范围内。

| 耗时字段 | 起止 |
|---|---|
| `admission_wait` | 消息准备到获得 mpsc permit；send 的背压等待 |
| `queue_residence` | 成功获得 permit 后记录入队时间，到消费者实际取出 |
| `ready_queue_wait` | 任务提交/从等待变为就绪，到进入下一次 poll；不包含 Future Pending 等待 |
| `ordered_wait` | blocking 等待同 Context 有序锁 |
| `worker_queue_wait` | blocking 提交到固定 Worker 池，到 Worker 取出 |
| `synchronous_elapsed` | 单次同步 poll/drop 或 blocking 执行 |
| `total_elapsed` | 任务提交到用户状态析构，包含异步等待；不能当成线程阻塞时间 |

慢操作阈值：EventLoop poll/drop 默认 100ms，Worker poll/drop 与 blocking 默认 1s，均可配置。
警告只会在同步操作返回后发出；操作尚未返回时，外部线程可以读取当前操作及 started_at。
没有常驻监控线程，不抢占用户代码，不提供“已强制终止”的保证。

结构化日志关联实例、部署、Context、任务、EventLoop、OS 线程以及 message_id/request_id/address；
错误提供稳定的 `VertxError::kind()`。消息 Span 随 handler 的每次 poll/drop 恢复，跨线程保留提交方 Dispatch。
消息 Span 保留发送方的因果父链；`message_handler` 子 Span 单独标明接收方 Context/Task，不能把父链当作当前 Context。
不输出消息体或全部 headers，库不安装 subscriber。示例支持 `RUST_LOG` 和 `LOG_FORMAT=json`。
默认摘要采集仍会在每次 poll 更新状态，并包装 Waker 跟踪就绪；轻量不等于零开销，可用 enabled=false 关闭采集。
队列摘要只统计当前已注册队列，注销即移除，累计满事件保留到实例结束。
满事件在非阻塞准入返回 Full，或一次异步准入首次等待且观察到容量耗尽时计数；重复 poll 不重复计数，Tokio 的协作让出本身不算队列满。
诊断注册表每个活跃队列保存一条弱引用及计数元数据，不强持有消息、sender、receiver 或 Context；详情上限限制快照输出。
注销后被外部保留的 Consumer 可能仍持有缓冲消息，直到 Consumer Drop 才释放；它们不再计入队列摘要。
容量按消息数量计算，待发送 Future、单条消息大小及任务数量均没有全局上限，不能据此宣称整个进程内存有界。
日志细节主要在 debug/trace；开启这些级别会增加开销，不承诺未经测量的性能提升。

本项目对通用风格中限制 `catch_unwind` 的规则保留框架边界例外。`panic=unwind` 时隔离 factory、生命周期回调构造 Future、
用户 Future 的 poll/drop、消息 handler、blocking 闭包和异常 hook 的调用及析构。遇到用户 panic 时会恢复 Context 并继续资源跟踪和清理。
`panic=abort` 或进程终止时没有展开过程可供框架捕获。
普通与 Worker 生命周期保留两个静态驱动，以保留 Send/!Send 差异；消息处理复用泛型循环，
不额外装箱 Future，不改变逐消息串行、Span/Dispatch 恢复或异常报告次数。

异常按 Context hook → 全局 hook 回退。调用、替换和清除都先释放配置锁，hook 及其析构可以重设注册。
框架错误事件独立于用户 hook，每个报告入口只输出一次。hook 自身的 panic 单独记录，避免递归。
生命周期结束与实例关闭时释放 hook 并收集析构错误，再继续完成清理。
受管任务自身的部署终止、实例关闭和显式取消不作为业务异常报告；读取其他部署的流而遇到终止，仍是失败并报告。

| 错误 | 用途 |
|---|---|
| 原始错误 | 只有一个失败时保持原类型 |
| `LifecycleFailed { cause, cleanup }` | 保留主错误及后续清理失败，包括 start/stop 和 pipe close，不覆盖首错 |
| `DeploymentFailed { cause, rollback }` | 仅用于多实例部署失败后的回滚 |
| `CleanupFailed` | close 汇总部署清理和 Worker/EventLoop join 错误 |

两个聚合错误的 cause 均实现标准 Error::source 链。Worker join 失败不能静默丢弃。
任务取消、业务失败、类型错误、请求超时、panic、关闭超时仍保持各自的结构化错误类型。

## 相关主题

[生命周期与关闭](lifecycle.md)、[验收与测试组织](testing.md)。
