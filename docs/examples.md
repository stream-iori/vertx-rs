# 示例与共享状态

[返回文档索引](index.md)

多 handler 共享状态优先级：单 handler 独占 → Rc<Cell> → 小作用域 Rc<RefCell> → 通过内部地址协调。
若 stop 需要访问任务使用的状态，通过这些明确的所有权结构共享；不要跨 await 保存 RefCell guard。

| 示例 | 内容 |
|---|---|
| [ping_pong](../examples/ping_pong.rs) | !Send 状态、线程/Context 身份、headers、业务失败、超时及迟到回复 |
| [pipeline](../examples/pipeline.rs) | pipe_to 有限源、容量 1 背压、Stream 收集、两个消费者、共享 Worker、有序/无序 blocking 及队列快照 |
| [verticle_to_verticle](../examples/verticle_to_verticle.rs) | 真实成功/失败服务、try_join_all / join_all / select_ok、取消及 detached task 说明 |

三个示例均使用 start/stop、可配置日志、有界等待和显式 close；操作与 close 都失败时保留两者。

## 相关主题

[生命周期与关闭](lifecycle.md)、[原生 Worker 调度与 Future 组合](worker-and-futures.md)。
