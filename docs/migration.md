# 从 0.2 迁移

[返回文档索引](index.md)

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
| Consumer::recv 返回 Option | 现在返回 Result<Option<Envelope<T>>>；先用 ? 处理终止错误，再处理 EOF |

多 handler 共享状态优先级：单 handler 独占 → Rc<Cell> → 小作用域 Rc<RefCell> → 通过内部地址协调。
若 stop 需要访问任务使用的状态，通过这些明确的所有权结构共享；不要跨 await 保存 RefCell guard。

| 示例 | 内容 |
|---|---|
| `ping_pong` | !Send 状态、线程/Context 身份、headers、业务失败、超时及迟到回复 |
| `pipeline` | pipe_to 有限源、容量 1 背压、Stream 收集、两个消费者、共享 Worker、有序/无序 blocking 及队列快照 |
| `verticle_to_verticle` | 真实成功/失败服务、try_join_all / join_all / select_ok、取消及 detached task 说明 |

三个示例均使用 start/stop、可配置日志、有界等待和显式 close；操作与 close 都失败时保留两者。

## 相关主题

[生命周期与关闭](lifecycle.md)、[原生 Worker 调度与 Future 组合](worker-and-futures.md)。
