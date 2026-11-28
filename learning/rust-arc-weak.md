# 用 Weak 打破 Arc 强引用环

两个对象需要互相访问时，可以让拥有方用 `Arc` 持有另一方，另一方用 `Weak` 回指。这样既能找到拥有方，也允许它在最后一份强引用释放后执行 `Drop`。

| 类型或方法 | 行为 |
|---|---|
| `Arc<T>` | 共享所有权。最后一份强引用被释放时，`T` 会被析构。 |
| `Arc::downgrade(&arc)` | 创建一个 `Weak<T>`，强引用计数保持不变。 |
| `weak.upgrade()` | 对象还活着时返回 `Some(Arc<T>)`，对象已析构时返回 `None`。 |

如果两个对象互相持有 `Arc`，即使外部句柄全部释放，两个对象仍会各自持有对方的一份强引用，引用计数都到不了零。把回指改为 `Weak`，就能打破这个强引用环。

## vertx-rs 的例子

```text
Vertx ────────── Arc ──→ VertxInner
VertxHandle ──── Weak ─→ VertxInner
Context ── Arc ─→ ContextData ── Weak ─→ VertxInner
```

- `VertxInner` 和 `ContextData` 通过 `Arc` 共用执行器、实例服务和关闭状态。
- 创建 `ContextData` 时，用 `Arc::downgrade()` 保存对 `VertxInner` 的弱引用。
- Verticle 接收的 `VertxHandle` 用弱引用访问实例。需要实例资源时才临时调用 `upgrade()`。应用可以把句柄克隆进任务，实例的强引用仍由 `Vertx` 持有。

[内部 Context](../src/context.rs) · [VertxHandle](../src/vertx/handle.rs) · [构建实例](../src/vertx/builder.rs)

## Clone 的复制深度

Rust 的 `Clone` 没有统一的深浅规则。类型自己决定怎样克隆。`#[derive(Clone)]` 会对结构体的每个字段调用该字段的 `clone()`，再用结果构造一个新值。

| 字段类型 | 克隆结果 |
|---|---|
| `bool` | 按值复制 |
| `String` | 复制字符串内容，新旧字符串可以各自修改 |
| 同样派生了 `Clone` 的内层结构体 | 继续逐字段调用内层字段的 `clone()` |
| `Arc<T>` | 新建一个共享句柄，强引用计数加一，两个句柄指向同一个 `T` |
| `Vec<Arc<T>>` | 新建一个向量，各元素克隆 `Arc` 句柄，底层的 `T` 仍共享 |

例如内部的 `Context::for_stop()` 先克隆一个 `Context`，再把副本的 `stopping` 设为 true。两个值各有一份 `stopping: bool`，通过各自的 `Arc` 共享同一份 `ContextData`。

```text
ctx.data           ──┐
                     ├──→ ContextData ──→ ExceptionSlot(RwLock<State>)
stop_ctx.data      ──┘
```

`ContextData`、`ExceptionSlot` 和 `RwLock` 都无需实现 `Clone`，因为 `Arc<T>` 的 `Clone` 实现无需克隆 `T`。通过其中一个 `Context` 设置异常回调，另一个也会读到同一个槽位。可以把这次 `Context::clone()` 称为浅克隆，判断依据是 `data` 字段的具体类型。

`ping_pong` 的心跳任务直接移动 timer 和 EventBus 句柄。业务代码需要在任务内继续调用框架时，可以克隆 `VertxHandle`，这会克隆弱引用。

[心跳任务](../examples/ping_pong.rs) · [Context 定义](../src/context.rs) · [ExceptionSlot 定义](../src/context/exception_handler.rs)

## Vertx 的 #[derive(Clone)]

`#[derive(Clone)]` 让编译器为 `Vertx` 生成 `Clone` 实现，依次克隆它的每个字段。目前唯一的字段是 `inner: Arc<VertxInner>`，所以生成的行为等价于下面的手写实现。 [Vertx 定义](../src/vertx.rs)

```rust
impl Clone for Vertx {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
```

`Arc::clone(&self.inner)` 增加强引用计数，返回一个指向同一份 `VertxInner` 的 `Arc`。所以 `vertx.clone()` 得到的是同一个运行时的另一个句柄，两个句柄共用线程池、EventBus 和部署表。通过任意一个句柄关闭实例，其他句柄也会看到同一次关闭。

`VertxInner` 本身无需实现 `Clone`。`Arc<VertxInner>` 这个字段类型决定了共享所有权，`#[derive(Clone)]` 则让外层 `Vertx` 句柄也能调用 `.clone()`。
