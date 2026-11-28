# EventBus

[返回文档索引](index.md)

`eventbus.rs` 导出消息类型。`bus.rs` 管 EventBus 句柄的归属和消费者注册。
地址路由、投递、消费和队列各自在对应子模块实现。

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

`DeliveryOptions` 给发送、广播、Sink 和回复配置 headers。请求用 `RequestOptions`，其中 timeout 覆盖入队和回复等待。
请求取消或超时后，已入队的消息仍可被消费。迟到 reply 返回 `ReplyClosed`。
Headers 按插入顺序保存，键大小写敏感且可重复。`get` 返回第一个值的 `&str`，`get_all` 遍历所有匹配的值。

## 归属与竞态

- Context::event_bus 注册始终绑定该 Context。
- Vertx::event_bus 在框架外注册为 unbound；在本实例的 Context 中注册时自动绑定当前 Context。
- 外部 unbound Consumer 使用 recv；需要 handler 时在所属 Context 内创建 Consumer。
- `handler` 在所属 EventLoop 逐条等待异步闭包完成。用 `async move |env|` 把状态交给回调，同一个回调可反复调用，也可以跨 `.await` 可变借用自己持有的 `!Send` 状态。
- `handler_worker` 使用 `FnMut -> Future`，闭包和返回的 Future 都要求 `Send`。两个入口都校验调用 Context。
- EventBus 路由表拥有订阅和队列。绑定消费者在部署作用域登记地址和注册 ID，生命周期可以按登记准确注销。
- 绑定部署时，在同一次准入内写入路由和作用域登记。进入 Stopping 后拒绝新订阅。
- 撤销部署只处理属于它的登记。关闭 EventBus 清除所有路由，包括框架外的注册。发送始终按地址查表。
- Consumer Drop 按地址和订阅 ID 注销单条记录。每次移除都和路由更新原子进行，移除地址下最后一个订阅时同时清除轮询状态。
- 注销先在路由锁内标记失活、移除目标并更新计数，再释放相关注册表锁，取消并释放 sender；Waker 可重入路由。

## 相关主题

[Stream、Sink 与背压传输](streams.md)、[生命周期与关闭](lifecycle.md)。
