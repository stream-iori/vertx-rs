# Tokio watch 通道

`tokio::sync::watch` 保存一个**最新值**。一个或多个 `Sender<T>` 可以更新它，多个 `Receiver<T>` 可以分别观察变化。创建通道时必须提供初始值。

```text
watch::channel(初始值)
        ↓
Sender 更新最新值 ──→ Receiver A
                   └─→ Receiver B
```

写入 `A → B → C` 时，慢接收者可能跳过 `B`，直接读到 `C`。适合用它同步配置、运行状态和关闭结果。每项都需要处理的工作可以交给 `mpsc` 队列。

## Sender 的能力

| 方法 | 行为 |
|---|---|
| `send(value)` | 写入新值并通知接收者。没有接收者时返回错误，本次更新不会保存。 |
| `send_replace(value)` | 写入新值并返回旧值。没有接收者时也会保存新值。 |
| `send_modify(f)` | 原地修改当前值并通知接收者。 |
| `send_if_modified(f)` | 原地修改当前值。闭包返回 `true` 时通知接收者。 |
| `subscribe()` | 创建接收者。它可以立即读取当前值。 |
| `closed().await` | 等到所有接收者都被释放。之后仍可用 `subscribe()` 新建接收者。 |

## Receiver 怎么读

- `borrow()` 读取当前值，保留这次更新的未读状态。
- `borrow_and_update()` 读取当前值，并把它标记为已读。
- `changed().await` 等待未读更新。已有未读值时立即返回，并将其标记为已读。新订阅者把订阅时的当前值视为已读，需要主动读取初始状态。
- 在 `changed().await` 循环里，通常用 `borrow_and_update()` 读取最新值，以免两次操作之间又发生更新，导致下一轮重复处理同一个值。读完及时释放借用，尤其要在下一个 `.await` 前释放。

## 项目里的 close_result

```rust
close_result: watch::Sender<Option<Result<()>>>
```

| 保存的值 | 含义 |
|---|---|
| `None` | 关闭尚无最终结果。 |
| `Some(Ok(()))` | 关闭成功，Worker 和 EventLoop 线程已完成回收。 |
| `Some(Err(error))` | 发起关闭或清理失败，保存错误供调用方读取。 |

`request_close()` 发起关闭。`wait_closed()` 订阅 `close_result` 后先读取当前值，看到 `None` 才等待更新。外部也可以用 `close()` 发起关闭并限时等待，超时后用 `wait_closed()` 接着等。 [等待结果](../src/vertx/shutdown.rs#L122)

清理流程先 join Worker，再由回收线程 join 所有 EventLoop。回收线程用 `send_replace()` 保存最终结果，即使当时没有等待者，之后订阅也能读到同一个结果。 [发布结果](../src/eventloop.rs#L182)
