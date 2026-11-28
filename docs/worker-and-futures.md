# 原生 Worker 调度与 Future 组合

[返回文档索引](index.md)

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
  裸 tokio::spawn 使用所进入的 Tokio runtime，既不继承 Context，也不成为原生 Worker 任务；用 VertxHandle::spawn 提交受管任务，框架外也可直接用 Vertx::spawn。
- 内部就绪队列不额外设置总容量；每个任务最多保留一个就绪通知。EventBus 的消费者队列默认容量为 256，可单独配置。
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

## 相关主题

[执行模型与 Context](execution-model.md)、[Timer 与配置](timers-and-config.md)。
