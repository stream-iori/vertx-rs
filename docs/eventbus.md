# EventBus

[返回文档索引](index.md)

```text
address → Route { TypeId, subscribers, round-robin cursor }
                       ↓
             每消费者 mpsc 队列，默认容量 256，可单独配置
                       ↓
                 Envelope<T>
                       ↓ 可选
             oneshot<Result<ErasedEnvelope>>
```

| API | 行为 |
|---|---|
| consumer<T> | 同地址存活消费者类型必须一致；冲突在注册时返回 TypeMismatch |
| consumer_with<T> | ConsumerOptions 配置消息数量上限；容量必须在 1..=Semaphore::MAX_PERMITS 内 |
| Consumer::recv / Stream | recv 返回 Result<Option<Envelope<T>>>；Stream 的 Item 为 Result<Envelope<T>> |
| send / send_with | 轮询选择一个消费者；等待该队列空位；成功表示入队 |
| try_send | 不等待；区分 QueueFull、ChannelClosed、NoConsumer |
| publish | 向所有消费者尝试取得许可，成功后才 Clone；满队列丢弃该次投递并记录丢弃数 |
| sink / sink_with | 地址绑定的 MessageSink<T>，实现 futures::Sink；后者配置固定 headers |
| request / request_with | 返回响应体；默认 30 秒；一个 deadline 覆盖入队和回复 |
| request_message / request_message_with | 返回完整响应 Envelope，可读取响应 headers |
| reply / reply_with | 一次性响应，后者支持响应 headers |
| fail(code, message) | 返回 RecipientFailure |
| into_parts | 拆出 body、Headers 和可跨任务移动的 ReplyHandle |

请求取消/超时不会撤回已经入队的业务消息；迟到 reply 返回 ReplyClosed。
Headers 按插入顺序保存、键大小写敏感、支持重复值；header 追加，insert 替换，get_all 读取全部值。

## 归属与竞态

- Context::event_bus 注册始终绑定该 Context。
- Vertx::event_bus 在框架外注册为 unbound；在本实例的 Context 中注册时自动绑定当前 Context。
- 外部 unbound Consumer 使用 recv；需要 handler 时在所属 Context 内创建 Consumer。
- handler 接受本地 !Send 闭包；handler_worker 要求 Send 闭包及 Future。都校验调用 Context。
- scope 存活检查、路由注册、作用域登记持有同一个准入锁；Stopping 不能插入新的登记。
- 路由注销使用 DashMap entry 原子更新，删除最后一个订阅时同时删除轮询状态。
- Consumer Drop 同时撤销路由和 scope 登记，避免长生命周期部署积累已销毁记录。
- 所有同步锁在 await 或用户回调前释放。
- 注销先在路由锁内标记失活、移除目标并更新计数，再于锁外取消和释放 sender；Waker 可重入路由。

## 相关主题

[Stream、Sink 与背压传输](streams.md)、[生命周期与关闭](lifecycle.md)。
