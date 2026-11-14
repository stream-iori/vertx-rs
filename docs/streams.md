# Stream、Sink 与背压传输

[返回文档索引](index.md)

```text
Stream<Result<T>> → 最多暂存一个元素 → Sink::poll_ready → start_send
                        ↑                    │
                        └──── Pending 时停止拉取 ┘
EOF → 等待 flush → 默认等待 close → 返回结果
失败 → 默认等待 close → 保留主错误与清理错误
```

| 环节 | 约定 |
|---|---|
| Consumer | 取消优先于同轮就绪的缓冲消息；作用域终止返回 DeploymentTerminated，unbound 全局关闭返回 VertxClosed |
| 终止 | 类型转换失败或取消只产出一次 Err，随后永久 None；正常 EOF 为 None；实现 FusedStream |
| MessageSink 就绪 | 首次 poll_ready 轮询选定消费者；Pending 后保持同一目标及排队位置，Ready 保留一个许可；重复 poll 不重复预留 |
| MessageSink 提交 | start_send 前再次检查目标存活；注销/关闭后不能复用缓存许可；支持 !Unpin 消息 |
| MessageSink flush | 确认已经入队，不等待业务处理完成；未使用的 readiness 许可保持到提交、close 或 Drop |
| MessageSink close/Drop | 仅结束当前适配器并释放排队/许可，不关闭共享地址或其他生产者 |
| pipe_to / pipe_to_with | 不创建任务或线程；不要求 Send/Unpin；保持调用方 Context，交给 spawn/spawn_local 管理 |
| 正常结束 | 始终等待 flush；PipeOptions.end_on_success 默认 true，随后等待 close |
| Result 失败 | PipeOptions.end_on_failure 默认 true，尝试 close；主错误与 close 错误同时保留在 LifecycleFailed |
| abort/timeout/Drop/panic | 释放尚未提交的元素和许可；不能异步 close，也不撤回已入队消息；panic 遵循调用方执行器处理 |

通用 pipe 每提交 64 个元素主动让出一次执行权，避免连续 Ready 的自定义流独占线程。
这不提供全局公平性、字节预算或吞吐保证。调用方借用 sink 并关闭 end_on_success 时，可在传输后继续使用它。
EventBus Consumer 通常不会自然 EOF，可用 take(n) 等组合器定义有限操作；部署终止不能当正常 EOF。
Envelope 的 body、headers、reply 能力必须显式处理；map_ok(into_body) 仅转发消息体，不自动传播 headers 或 reply。
sink_with 的 timeout 与 send_with 一样仅用于 request API，不是排队 deadline；需要时由调用方包裹 timeout。
同消费者的 Future handler 串行规则不变；同步慢操作仍应交给 execute_blocking。

## 相关主题

[EventBus](eventbus.md)、[生命周期与关闭](lifecycle.md)。
