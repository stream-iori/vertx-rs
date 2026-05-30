# vertx-rs 设计文档

> 目标:用 Rust + Tokio 复刻 Vert.x core 的执行模型 ——
> **EventLoop 亲和的 Verticle** + **多线程 WorkerVerticle** + **Context** + **EventBus** + **Timer**。
>
> 范围:**单进程**,不含集群、不含 HTTP/Web 组件、不含分布式 EventBus。
> Vert.x core 中已对齐的核心子集见 §10。

---

## 0. 环境要求

| 项 | 版本 / 说明 |
|---|---|
| Rust toolchain | MSRV 1.75+;实际开发用 stable 1.96.0 |
| 运行时 | Tokio 1.x(`rt-multi-thread` + `rt` + `sync` + `macros` + `time`) + `tokio-util/rt`(`LocalPoolHandle`) |
| Edition | 2021 |

---

## 1. 设计哲学

借鉴 Vert.x 的两条核心原则,并完整复刻 EventLoop 亲和性:

1. **Don't block the event loop** —— Verticle 在它的 EventLoop 线程上跑,任何阻塞都必须走 WorkerVerticle 或 `Context::execute_blocking`。
2. **No shared mutable state** —— Verticle 之间**只能**通过 EventBus 通信(Rust 类型系统强制)。

但**用更地道的 Rust 风格表达**:

- **Verticle 是 owned-self actor,而不是 start/stop 双钩对象** —— `async fn run(self, ctx) -> Result<()>` 是唯一的方法。Verticle 实例被移动进框架的 task,**它自己就是那个 task**。状态可以是普通字段(`HashMap`、`Cell`),不必包 `Rc<RefCell<_>>`。
- **生命周期 = run 的 future** —— deploy 后框架轮询这个 future,undeploy 触发 `CancellationToken`,run 的 `select!` 分支醒来退出,清理代码就是 fn 末尾的几行。框架不再外部编排 start/stop 两阶段。
- **Factory 模式负责"ready"** —— `deploy(factory)` 中的 `factory: FnOnce(Context) -> V` 在目标 EventLoop 上**同步**运行,典型用途是注册 EventBus consumer。factory 返回 = verticle ready,`deploy` 返回 = 消息可以安全送达。

与 Vert.x 的实质差异:

- **不做动态字节码部署**:Verticle 是编译期已知的 Rust 类型,factory 闭包替代 `Class<? extends Verticle>`。
- **用户不能选 EventLoop**:`deploy` 时由框架 RR 内部分派,亲和性是不可篡改的运行时不变量。
- **没有 start/stop 两钩**:owned-self / 单个 `run` 更适合 Rust 的所有权模型(详见 §3.1)。

---

## 2. 顶层架构

```
┌──────────────────────────────────────────────────────────────────┐
│                          Vertx (句柄)                             │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐      │
│  │                  EventBus Router (共享)                  │      │
│  │   address -> Vec<SubscriberHandle { mpsc::Sender }>     │      │
│  └────────────────────────────────────────────────────────┘      │
│                                                                  │
│  ┌──────────────┐ ┌──────────────┐ ┌────────────────────────┐    │
│  │ EventLoop #0 │ │ EventLoop #1 │ │   Worker Pool          │    │
│  │ (LocalSet)   │ │ (LocalSet)   │ │ (multi-thread tokio)   │    │
│  │              │ │              │ │                        │    │
│  │ Verticle A   │ │ Verticle C   │ │ WorkerVerticle X       │    │
│  │ Verticle B   │ │ Verticle D   │ │ WorkerVerticle Y       │    │
│  └──────────────┘ └──────────────┘ └────────────────────────┘    │
│       ↑ N = available_parallelism()      ↑ 默认 20,可配置         │
└──────────────────────────────────────────────────────────────────┘
```

- **EventLoop Pool**:`tokio_util::task::LocalPoolHandle::new(N)`,每个 EventLoop = 一个 OS 线程 + 一个 `LocalSet`。
- **Worker Pool**:独立 multi-thread Tokio Runtime,默认 20 worker 线程,完全隔离。
- **Verticle 部署时被钉到某个 EventLoop**(框架 RR 选定),终生不变;状态可以是 `!Send`(`Rc`、`RefCell` 等)。
- **WorkerVerticle 部署在 Worker Pool**,无亲和性,状态必须 `Send`。
- **EventBus Router 是全局共享的**;但每个 Verticle 拿到的 **EventBus 句柄是绑定到自己 Deployment 的**(见 §3.4)。

---

## 3. 核心 API

### 3.1 Verticle / WorkerVerticle

```rust
/// EventLoop-affine Verticle. owned self,不要求 Send。
#[async_trait(?Send)]
pub trait Verticle: 'static {
    async fn run(self, ctx: Context) -> Result<()>;
}

/// 多线程 Worker Verticle. owned self,要求 Send。
#[async_trait]
pub trait WorkerVerticle: Send + 'static {
    async fn run(self, ctx: Context) -> Result<()>;
}
```

两个 trait 各有一个方法,差别只在 trait bound。**Verticle 实例被 move 进框架的 task**,它**就是**那个 task。

典型实现形态:

```rust
async fn run(mut self, ctx: Context) -> Result<()> {
    let cancel = ctx.cancellation_token();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),   // 协作式终止
            msg = self.consumer.recv() => {
                let Some(env) = msg else { return Ok(()) };
                // ... handle ...
            }
        }
    }
    // 如果 run 的末尾还有清理代码,放这里
}
```

**为什么 owned self,不是 `&mut self` start/stop**:Rust 的所有权系统对"对象被移动进 task,task = 对象的全部生命周期"咬合得最干净 —— 状态可以是普通 `Cell<u64>`、`HashMap<K, V>` 不需要 `Rc<RefCell<_>>`,清理代码就是 fn 末尾的代码,框架不需要外部编排两阶段。这是相对 Vert.x 最显著的 Rust 化改写。

### 3.2 Context

```rust
#[derive(Clone)]
pub struct Context { /* opaque */ }

pub enum ThreadingModel { EventLoop, Worker, External }

impl Context {
    // —— Vert.x 对齐的内省 ——
    pub fn is_event_loop(&self) -> bool;
    pub fn is_worker(&self) -> bool;
    pub fn threading_model(&self) -> ThreadingModel;       // 对齐 Vert.x
    pub fn event_loop_id(&self) -> Option<EventLoopId>;
    pub fn deployment_id(&self) -> Option<DeploymentId>;   // 对齐 Vert.x
    pub fn owner(&self) -> Option<Vertx>;                  // 对齐 Vert.x

    // —— 生命周期 ——
    pub fn is_alive(&self) -> bool;
    pub fn cancellation_token(&self) -> CancellationToken;

    // —— EventBus & Timer 句柄(均绑定到本 Verticle scope) ——
    pub fn event_bus(&self) -> EventBus;
    pub fn timer(&self, delay: Duration) -> Timer;          // 见 §3.5
    pub fn periodic(&self, init: Duration, period: Duration) -> Periodic;

    // —— 调度 ——
    /// 把 f 调度回这个 Context 上跑。**Vert.x 风格 fire-and-forget**。
    /// 需要等结果的话用户自己 oneshot。
    pub fn run_on_context<F>(&self, f: F) -> Result<()>
        where F: FnOnce() + Send + 'static;

    pub async fn execute_blocking<F, R>(&self, f: F) -> Result<R>
        where F: FnOnce() -> R + Send + 'static, R: Send + 'static;

    /// 在本 Context 的 LocalSet 上 spawn 一个 !Send future。
    /// 仅 EventLoop Context 可用,Worker Context 调用 panic。
    /// 这是 Verticle 内启动子任务的推荐入口 —— 把"必须用 spawn_local"
    /// 这种 framework knowledge 显式化为 API。
    pub fn spawn_local<F>(&self, fut: F)
        where F: Future<Output = ()> + 'static;

    // —— 当前线程探测(静态),对齐 Vert.x ——
    pub fn current() -> Option<Context>;
    pub fn is_on_event_loop_thread() -> bool;
    pub fn is_on_worker_thread() -> bool;
    pub fn is_on_vertx_thread() -> bool;
}
```

Context 是**不透明结构**,用户只能查询/调用,不能 destructure 或构造。**生命周期与产生它的 Verticle 绑定**:见 §4.5。

> **`run_on_context` 与 Vert.x 对齐**:Vert.x 的 `Context.runOnContext(handler)` 是 `void` 返回值的 fire-and-forget,handler 异步执行。我们的签名也是 fire-and-forget,但返回 `Result<()>` 区分"调度入队成功"和"verticle 已撤销"。

> **`execute_blocking` 与 Vert.x 对齐**:Vert.x 的 `executeBlocking` 返回 `Future<T>`,完成后**回到原 Context**。我们用 Tokio LocalSet 天然得到这个性质 —— 在 EventLoop verticle 内 await 它,continuation 必然回到该 LocalSet 的线程。`ordered` 参数(Vert.x 提供)目前不支持。

### 3.3 Vertx 句柄

```rust
impl Vertx {
    pub fn new() -> Self;                    // 默认值
    pub fn builder() -> VertxBuilder;        // 自定义 EventLoop 数 / Worker 数

    /// 框架外的"全局"句柄(unbound,不与任何 Verticle 关联)。
    /// 适合测试 / main 中驱动消息。
    pub fn event_bus(&self) -> EventBus;

    pub fn event_loop_count(&self) -> usize;
    pub fn worker_thread_count(&self) -> usize;

    /// 框架内部 RR 选 EventLoop;用户不能指定。
    /// factory 在目标 EventLoop 线程上**同步**调用,接收 Context;典型用途
    /// 是在 factory 里注册 EventBus consumer。返回 Verticle 实例后,
    /// `deploy` 立即返回 —— 此时 verticle 已 ready,后续 run() 被框架轮询。
    pub async fn deploy<F, V>(&self, factory: F) -> Result<DeploymentId>
        where F: FnOnce(Context) -> V + Send + 'static, V: Verticle;

    /// WorkerVerticle 直接传实例(本来就 Send)。
    pub async fn deploy_worker<V: WorkerVerticle>(&self, v: V) -> Result<DeploymentId>;

    /// 撤销部署。cancel scope → 清扫 bus → 触发 CancellationToken → 等 run()
    /// 自然退出;1s 内不退出则 abort 强制中止(对付不监听 cancel 的 verticle)。
    pub async fn undeploy(&self, id: DeploymentId) -> Result<()>;

    /// 全局(unbound)Timer / Periodic。生命周期独立,不跟 Verticle。
    pub fn timer(&self, delay: Duration) -> Timer;
    pub fn periodic(&self, initial_delay: Duration, period: Duration) -> Periodic;
}

impl VertxBuilder {
    pub fn event_loop_threads(self, n: usize) -> Self;  // 默认 available_parallelism()
    pub fn worker_threads(self, n: usize) -> Self;      // 默认 20
    pub fn build(self) -> Vertx;                        // 失败 panic(框架级)
}
```

**为什么 deploy 用 factory(Context)**:Verticle 实例本身可能含 `Rc`,无法跨线程移动。`FnOnce(Context) -> V + Send` 的闭包**是** Send,跨线程发到目标 EventLoop 后调用产出 `V`;Context 传进来让 factory **同步**注册 EventBus consumer,这样 `deploy` 返回时 consumer 已就位,后续从外部 send 不会因为消息到达过早而 NoConsumer。

### 3.4 EventBus 与 Verticle 的绑定

EventBus 内部的路由表是**全局共享**的(否则 Verticle 之间无法通信),但用户拿到的 **EventBus 句柄分两种**:

| 来源 | 类型 | 生命周期约束 |
|---|---|---|
| `Vertx::event_bus()` | **unbound** | consumer 一直存活,直到 `Consumer<T>` drop |
| `Context::event_bus()` | **bound to deployment** | consumer 在 `undeploy` 时自动取消订阅 |

```rust
impl EventBus {
    pub fn publish<T: Clone + Send + 'static>(&self, address: &str, body: T) -> Result<()>;
    pub async fn send<T: Send + 'static>(&self, address: &str, body: T) -> Result<()>;
    pub fn try_send<T: Send + 'static>(&self, address: &str, body: T) -> Result<()>;
    pub async fn request<T, R>(&self, address: &str, body: T) -> Result<R>;
    pub async fn request_with<T, R>(&self, address: &str, body: T, opts: DeliveryOptions) -> Result<R>;
    pub fn consumer<T: Send + 'static>(&self, address: &str) -> Result<Consumer<T>>;
}

impl<T> Consumer<T> {
    pub async fn recv(&mut self) -> Option<Envelope<T>>;
    /// Vert.x-style: spawn a task that pulls envelopes and feeds them to `handler`.
    pub fn handler<F, Fut>(self, handler: F)
    where
        F: FnMut(Envelope<T>) -> Fut + 'static,
        Fut: Future<Output = Result<()>> + 'static;
}
```

`consumer` 在 bound 句柄上若 verticle 已撤销返 `DeploymentTerminated`,unbound 句柄一定 Ok。
`request` 用 `DeliveryOptions` 默认值(30s timeout,对齐 Vert.x);需要自定义 timeout / 后续 headers 时用 `request_with`。
`Consumer<T>::handler(fn)` 是 Vert.x faithful 的 handler 注册形态——内部 `spawn_local` 一个 `while let Some(env) = c.recv().await` 循环,跟 `bus.consumer(addr).handler(msg -> {...})` 语义对齐。需要多源串行 dispatch + `&mut self` 时,仍用底层的 `Verticle::run` + `tokio::select!`。

### 3.4.1 Envelope / ReplyHandle

```rust
pub struct Envelope<T> { /* opaque */ }

impl<T> Envelope<T> {
    pub fn body(&self) -> &T;
    pub fn body_mut(&mut self) -> &mut T;
    pub fn headers(&self) -> &HashMap<String, String>;
    pub fn has_reply(&self) -> bool;

    /// In-place reply, consuming self. 常用快捷写法。
    pub fn reply<R: Send + 'static>(self, resp: R) -> Result<()>;

    /// 拆成三部分:把 body 留给一个任务,reply handle 交给另一个任务。
    pub fn into_parts(self) -> (T, HashMap<String, String>, Option<ReplyHandle>);

    /// 只要 body。
    pub fn into_body(self) -> T;
}

/// 独立的 reply 句柄,可跨 task 传递。
pub struct ReplyHandle { /* opaque */ }

impl ReplyHandle {
    pub fn reply<R: Send + 'static>(self, resp: R) -> Result<()>;
}
```

`send` / `publish` / `request` 在两类句柄上行为完全一致(都走同一路由器);唯一差别在 `consumer`:**bound 句柄注册的 consumer 会记账在该 Verticle 的 DeploymentScope 上**,`undeploy` 时一次性清扫。

跨上下文派发不需要任何额外代码 —— `mpsc::Sender` 是 `Send`,publisher 从任意线程 send,receiver 在 Verticle 自己的 EventLoop 上 recv。

### 3.4.2 Handler 状态选型

Vert.x 里 `start()` 注册的 handler 闭包捕获 `this`,所有 handler 共享 verticle 字段——JVM 无借用检查,这是常态。Rust 这边一个 verticle 可能多个 handler 都要可变状态,编译期 `&mut self` 互斥,所以必须做选型。**按力度从弱到强(也是优先级从高到低)选**:

| 场景 | 推荐 | 备注 |
|---|---|---|
| 单 handler 独占状态 | `move` + `FnMut`,直接拿可变捕获变量 | 编译期保证,零开销。**首选** |
| 多 handler 共享 `Copy` 标量 | `Rc<Cell<T>>` | 无 borrow guard,无 panic 风险 |
| 多 handler 共享复合状态 (`HashMap`/`Vec`) | `Rc<RefCell<T>>` | 单线程 EventLoop 上 borrow 不跨 await 时安全;**borrow scope 越小越好** |
| 多 handler 需协同但状态可拆 | EventBus 内部地址互发消息 | actor 派系,无共享可变 |

#### `Rc<RefCell<T>>` 的纪律

合理用法存在(图结构、注册中心、跨 handler 共享 cache),但要遵守两条硬规矩:

1. **每个 `borrow()` / `borrow_mut()` 只跨一个语句**,出函数前显式 `drop(guard)` 或用 `{...}` 块限定作用域。
2. **永远不要跨 `.await` 持有 borrow guard**——`RefMut` 跨 await 不会被编译器拦截(`!Send` 在 `spawn_local` 下合法),但下一个被调度的 handler 一旦 `borrow_mut` 就会 panic。

```rust
// ✅ 正确:borrow 作用域 = 一个表达式
let n = {
    let mut m = counts.borrow_mut();
    let n = m.entry(key).or_insert(0);
    *n += 1;
    *n
};
async move { env.reply(n) }.await

// ❌ 错误:borrow 跨 await
let mut m = counts.borrow_mut();
m.insert(k, async_compute().await);    // 跨 await,可能 panic
```

不遵守这两条规矩时,把它当成"程序里埋了一颗运行时炸弹"。Rust 把借用检查推迟到了运行时,失败模式从 cargo build 报错变成了生产 panic。

### 3.5 Timer 与 Periodic

```rust
/// One-shot timer. 实现 Future<Output = Result<()>>。
pub struct Timer { /* opaque */ }

impl Timer {
    pub fn cancel(&self);
    pub fn is_cancelled(&self) -> bool;
}
impl Future for Timer { type Output = Result<()>; /* ... */ }

/// 周期 timer。
pub struct Periodic { /* opaque */ }

impl Periodic {
    pub async fn tick(&mut self) -> Result<()>;
    pub fn cancel(&self);
    pub fn is_cancelled(&self) -> bool;
}
```

来源分两种(与 EventBus 句柄结构对称):

| 入口 | 是否绑定 Verticle | 行为 |
|---|---|---|
| `Vertx::timer / Vertx::periodic` | **unbound** | 仅显式 `cancel()` 或 await 自然完成 |
| `Context::timer / Context::periodic` | **bound to deployment** | 同上 + undeploy 时**自动取消**(in-flight await 收到 `TimerCancelled`) |

> **与 Vert.x 对齐**:Vert.x 5.x 的 `Vertx::timer(delay)` 返回的 `Timer` 是 `Future<Void>` + `Cancelable`,与本设计形态一致。我们故意不提供旧 API `setTimer(delay, handler) -> long id`,因为 Rust 里 callback 风格不自然,且 `Timer::cancel()` 已经替代了 `cancelTimer(id)` 的功能。

> **`tick()` 而非 `Stream`**:`Periodic` 没实现 `Stream` trait —— 因为 `Stream::next` 的 lifetime 形态对取消语义不友好。`tick(&mut self) -> Result<()>` 更直接:每次显式 await,可以在错误时退出循环。

---

## 4. 关键实现细节

### 4.1 EventLoop 派发

用 `tokio_util::task::LocalPoolHandle::spawn_pinned_by_idx(idx, factory)`:
- `factory: FnOnce() -> Future + Send` —— 工厂可跨线程
- 工厂在目标线程上调用,产出的 `Future` 不需要 `Send`
- 这就是部署 `!Send` Verticle 的底层机制

每次 deploy:
1. 框架内部 RR 选一个 `EventLoopId`(用户**不可指定**)
2. 分配 `DeploymentId`,创建 `DeploymentScope`(空 subscriber 列表)
3. `spawn_pinned` 一个 future,future 内部:`let mut v = factory(); v.start(ctx).await; cancel.cancelled().await; v.stop().await;`
4. 通过 oneshot 把 `start` 的结果送回 deploy 调用方

### 4.2 DeploymentScope 与 bound EventBus

```rust
pub(crate) struct DeploymentScope {
    deployment: DeploymentId,
    subscribers: Mutex<Vec<(String, SubscriberId)>>,
}

pub struct EventBus {
    inner: Arc<EventBusInner>,
    scope: Option<Arc<DeploymentScope>>,  // None = unbound, Some = bound
}
```

- `Context::event_bus()` → `inner.event_bus.bound(scope.clone())`,产出 `scope=Some`
- 在 bound 句柄上调用 `.consumer(addr)`:`inner.register(addr)` 拿到 `SubscriberId`,然后 push 到 `scope.subscribers`
- `Vertx::undeploy(id)`:先 `bus.cleanup_scope(scope)` 把所有 `(addr, sub_id)` 一次性从路由表 `unregister`,再 `cancel.cancel() + join.await`
- 即使用户**没**主动 drop `Consumer<T>`,undeploy 后路由也是干净的

`Consumer<T>::Drop` 仍然在(供 unbound 场景)。它和 scope cleanup 互相幂等(`retain` 找不到 id 就静默)。

### 4.3 Worker Pool 派发

`WorkerPool` 内部持有 `Arc<OwnedRuntime>`,`OwnedRuntime: Drop` 时调 `Runtime::shutdown_background()` —— 在 `#[tokio::test]` 等"嵌套 runtime"场景下析构不会 panic。

- `deploy_worker`:`rt.spawn(start_future)`,再 `rt.spawn(cancel.cancelled() then stop_future)`
- `execute_blocking`:`rt.spawn_blocking(f)`,用的是 worker runtime 的 blocking pool

### 4.4 Context 的 thread-local

```rust
thread_local! {
    static CURRENT: RefCell<Option<Context>> = RefCell::new(None);
}
```

- 进入 Verticle 时通过 `ContextGuard`(RAII)设置 CURRENT
- `Context::current()` 在 Vertx 设置过的线程上返回 `Some(ctx)`,否则 `None`
- `ContextGuard::drop` 还原 prev,支持嵌套

### 4.5 Context 与 Verticle 的生命周期绑定

Context 可被任意 clone 并越过 Verticle 自身的边界(传给 main、传给 WorkerVerticle、传给纯 std::thread 等)。但**它的"调度能力"必须随 Verticle 的撤销而失效**,否则会出现"verticle 已死、Context 继续往那个 EventLoop 派发空闲任务"的悬空状态。

实现:

```rust
pub(crate) struct DeploymentScope {
    deployment: DeploymentId,
    alive: AtomicBool,          // 翻 false 后所有调度入口立即拒绝
    cancel: CancellationToken,  // 协作式终止信号
    subscribers: Mutex<Vec<(String, SubscriberId)>>,
}
```

每个 Verticle 部署时分配一份 `Arc<DeploymentScope>`,该 Arc 同时被三方持有:
1. `Deployment`(存储在 Vertx 的 deployments 表里)
2. 该 Verticle 自己的 Context(以及任何 clone 出去的副本)
3. 该 Verticle 通过 `ctx.event_bus()` 拿到的所有 bound EventBus 句柄

**`undeploy` 严格按顺序执行**:

```rust
async fn undeploy(&self, id: DeploymentId) -> Result<()> {
    let (_, dep) = self.inner.deployments.remove(&id)?;
    // 1. 翻死 alive,新的 run_on_context / execute_blocking / bound consumer
    //    立刻拒绝
    dep.scope.mark_dead();
    // 2. 清扫 bus 订阅,verticle 的 recv 循环醒来收到 None,自然退出
    self.inner.shared.event_bus.cleanup_scope(&dep.scope);
    // 3. 触发 CancellationToken,verticle 的 run() select! 分支收到信号
    dep.scope.cancel.cancel();
    // 4. 等 run() 自然退出,1s 内不退则 abort 兜底
    match tokio::time::timeout(UNDEPLOY_GRACE, dep.join).await {
        Ok(_) => Ok(()),
        Err(_) => { abort_handle.abort(); Ok(()) }
    }
}
```

**Context 调度入口的检查**:`run_on_context` / `execute_blocking` 进入时先 `ensure_alive()`;`run_on_context` 还在跨线程到达目标后**再次检查**,以闭合"调度入队 → 翻死"中间的小窗口。

**bound consumer 注册**:`EventBus::consumer` 在 `scope.is_alive` 为 false 时直接返 `DeploymentTerminated`,并在 register 之后再做一次 re-check 防竞态。

**用户 run() 的协作式取消**:用 `tokio::select!` 显式监听 `ctx.cancellation_token().cancelled()`,这样 undeploy 的 1s 宽限期内 run() 就能自然退出。不监听 cancel 也不会死锁 —— bus consumer 被 cleanup_scope 关闭,`recv()` 返 None;但纯 CPU 循环就要靠 abort 兜底,**多一个 cancel 分支是最佳实践**。

```rust
async fn run(mut self, ctx: Context) -> Result<()> {
    let cancel = ctx.cancellation_token();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),   // ← 协作式
            msg = self.consumer.recv() => {
                let Some(env) = msg else { return Ok(()) };
                // ...
            }
        }
    }
}
```
不监听 token 也不会"卡死":consumer recv 会被 cleanup_scope 唤醒返 None。监听 token 是更显式、更早响应的写法。

**unbound Context / unbound EventBus**(来自 `Vertx::event_bus()`)永远 alive,`cancellation_token` 返回的是从不取消的 fresh token,`try_consumer` 永远成功。这是因为它不绑定到任何 Verticle 上,只能由 Vertx 整体 drop 时收尾。

---

## 5. 典型用法

### 5.1 owned-self Verticle 持普通字段(无 Rc/RefCell)

```rust
struct Pinger {
    consumer: Consumer<String>,
    count: Cell<u64>,    // 普通字段,不是 Rc<RefCell<_>>
}

#[async_trait(?Send)]
impl Verticle for Pinger {
    async fn run(mut self, ctx: Context) -> Result<()> {
        let cancel = ctx.cancellation_token();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                msg = self.consumer.recv() => {
                    let Some(env) = msg else { return Ok(()) };
                    self.count.set(self.count.get() + 1);
                    let _ = env.reply(format!("pong#{}", self.count.get()));
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let vx = Vertx::new();
    vx.deploy(|ctx| Pinger {
        consumer: ctx.event_bus().consumer("ping").unwrap(),
        count: Cell::new(0),
    }).await?;
    let _: String = vx.event_bus()
        .request("ping", "hi".to_string())
        .await?;
    Ok(())
}
```

### 5.2 Verticle 之间通过 bound EventBus 通信

```rust
struct CallerV {
    consumer: Consumer<String>,
    bus: EventBus,           // bound bus,在 factory 里就抓出来
}

#[async_trait(?Send)]
impl Verticle for CallerV {
    async fn run(mut self, ctx: Context) -> Result<()> {
        let cancel = ctx.cancellation_token();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                msg = self.consumer.recv() => {
                    let Some(env) = msg else { return Ok(()) };
                    let upper: String = self.bus
                        .request("upper", env.body().clone())
                        .await?;
                    let _ = env.reply(upper);
                }
            }
        }
    }
}
```

### 5.3 `execute_blocking` 跨上下文跳转

```rust
async fn run(mut self, ctx: Context) -> Result<()> {
    let cancel = ctx.cancellation_token();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            msg = self.consumer.recv() => {
                let Some(env) = msg else { return Ok(()) };
                let n = *env.body();
                // EventLoop 上调用 execute_blocking,跳 worker 算重活,
                // await 完成后 LocalSet 保证 continuation 回到原 EventLoop。
                let result = ctx.execute_blocking(move || expensive(n)).await?;
                let _ = env.reply(result);
            }
        }
    }
}
```

### 5.4 把 reply handle 交给另一个任务

`Envelope::into_parts` 让你把 body / headers / reply handle 拆开。配合 `ctx.spawn_local`,可以"主循环立即返回,reply 在后台 task 算完再发":

```rust
async fn run(mut self, ctx: Context) -> Result<()> {
    let cancel = ctx.cancellation_token();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            msg = self.consumer.recv() => {
                let Some(env) = msg else { return Ok(()) };
                let (job, _h, reply) = env.into_parts();
                let ctx2 = ctx.clone();
                ctx.spawn_local(async move {
                    let r = ctx2.execute_blocking(move || job.process()).await;
                    if let (Some(h), Ok(v)) = (reply, r) { let _ = h.reply(v); }
                });
            }
        }
    }
}
```

---

## 6. 模块划分

```
src/
├── lib.rs               // re-export
├── vertx.rs             // Vertx, VertxBuilder, deploy / deploy_worker / undeploy
├── verticle.rs          // Verticle (?Send), WorkerVerticle (Send)
├── context.rs           // Context (opaque), thread-local CURRENT, ContextGuard
├── eventloop.rs         // EventLoopId, EventLoopPool (LocalPoolHandle 包装)
├── worker_pool.rs       // WorkerPool, OwnedRuntime (shutdown_background)
├── deployment.rs        // DeploymentId
├── error.rs             // VertxError / Result
└── eventbus/
    ├── mod.rs           // EventBus 公共 API + DeploymentScope + bound/cleanup
    ├── inner.rs         // 路由表 + RR 调度
    ├── envelope.rs      // Envelope<T> + ErasedEnvelope
    └── consumer.rs      // Consumer<T> + Drop unregister
```

---

## 7. 验收清单

- [x] `cargo test --tests` 全绿(26/26)
  - [x] `deploy` 在 factory 返回后立即 ready;未知 id `undeploy` → `DeploymentNotFound`
  - [x] **不监听 cancel 的 run() 会被 abort 兜底**
  - [x] EventLoop 亲和性、owned `Cell<u64>` 字段(无 Rc/RefCell)、Verticle 间通信
  - [x] WorkerVerticle、`execute_blocking` 回 EventLoop
  - [x] `Context::current()` / 内省(EventLoop 与 Worker 两路径)
  - [x] `Context::threading_model() / deployment_id() / owner()`(Vert.x 对齐)
  - [x] `Context::is_on_event_loop_thread() / is_on_worker_thread() / is_on_vertx_thread()`(Vert.x 对齐)
  - [x] `run_on_context` fire-and-forget,从外部线程跳进 EventLoop
  - [x] **undeploy 后 `run_on_context` / `execute_blocking` / bound `consumer` 全部返 `DeploymentTerminated`**
  - [x] `Vertx::timer` 正常触发与显式 cancel;`Vertx::periodic` 多次 tick + cancel
  - [x] `Context::timer` 在 undeploy 时自动 cancel(bound timer 不漏)
  - [x] `VertxBuilder` 覆盖默认值,EventBus 核心(publish / send / request + DeliveryOptions / consumer drop)
- [x] `examples/ping_pong.rs`(owned `Cell<u64>`)
- [x] `examples/pipeline.rs`(Source/Doubler×2/Sink 都是 owned-self run loop)
- [x] `examples/verticle_to_verticle.rs`(bound EventBus 互相调用)
- [ ] 性能 sanity check(后续)

---

## 8. 明确不做(范围外)

- 集群 / 分布式 EventBus
- `Codec<T>` 与跨进程序列化
- HTTP / WebSocket / TCP 服务端
- 用户**指定 EventLoop 部署**(`deploy_on` 不存在,框架 RR 内部分派)
- Verticle **跨 EventLoop** 启动 task(只能在自己的 LocalSet 上 `tokio::task::spawn_local`)
- 动态加载 / 热部署
- Metrics / 健康检查端点(只埋 `tracing` 钩子)

---

## 9. 已落地的决定

| 决定 | 选择 | 理由 |
|---|---|---|
| EventLoop 数量默认 | `std::thread::available_parallelism()` | 跟 Vert.x 一致;可被 builder 覆盖 |
| Worker 线程数默认 | 20(可覆盖) | 贴近 Vert.x `setWorkerPoolSize` 默认值 |
| 用户指定 EventLoop | **不支持**,框架 RR 内部分派 | 亲和性是运行时不变量,不应让用户扰动 |
| Verticle 跨 EventLoop 启 task | 不支持 | 保持亲和性语义清晰;跨上下文用 `execute_blocking` |
| Verticle 通信方式 | 各 Verticle 用 `ctx.event_bus()` 拿 **bound** 句柄,通过 address 互相调用 | 自动随 undeploy 清理 consumer |
| `publish` 多订阅者 | `T: Clone` 路径 | 集群阶段再走 Codec |
| Worker Runtime 析构 | `shutdown_background` | 兼容 `#[tokio::test]` 嵌套 runtime |

---

## 10. 与 Vert.x core 的对照

本节列出当前实现相对 Vert.x core 5.x 的覆盖情况,聚焦 **EventLoop / Context / Worker / EventBus / Timer** 五项基础能力。

### 10.1 已对齐

| 能力 | Vert.x | vertx-rs | 备注 |
|---|---|---|---|
| 多 EventLoop | `setEventLoopPoolSize`,默认 2*CPU | `VertxBuilder::event_loop_threads`,默认 `available_parallelism` | Vert.x 默认 2x,我们默认 1x,够用 |
| Worker 池 | `setWorkerPoolSize`,默认 20 | `worker_threads(N)`,默认 20 | ✅ 数值一致 |
| Verticle 部署 | `deployVerticle(supplier, options)` 有 start/stop 双钩 | `deploy(factory(ctx) -> V)` + Verticle::run(self, ctx) **owned-self** | Rust 化改写,见 §3.1 |
| Worker Verticle | `DeploymentOptions::setThreadingModel(WORKER)` | `deploy_worker(v)` | 拆成独立 API,更显式 |
| Context.runOnContext | `void runOnContext(Handler<Void>)` | `run_on_context(f) -> Result<()>` | fire-and-forget 对齐,Result 多一个调度失败信号 |
| Context.executeBlocking | `Future<T> executeBlocking(Callable<T>)`,回原 context | `async execute_blocking<F,R>(f) -> Result<R>` | LocalSet 天然回原线程 |
| Context.deploymentID | `String deploymentID()` | `deployment_id() -> Option<DeploymentId>` | ✅ |
| Context.owner | `Vertx owner()` | `owner() -> Option<Vertx>` | ✅(Weak 升级) |
| Context.threadingModel | `ThreadingModel threadingModel()` | `threading_model() -> ThreadingModel` | ✅ |
| 静态线程探测 | `Context.isOnEventLoopThread()` 等 | `Context::is_on_event_loop_thread()` 等 | ✅ |
| EventBus.send | `EventBus.send(addr, msg)` | `EventBus::send(addr, msg).await` | async + 背压 |
| EventBus.publish | `EventBus.publish(addr, msg)` | `EventBus::publish(addr, msg)` | 需 `T: Clone`(MVP 限制) |
| EventBus.request | `Future<Message<T>> request(addr, msg[, opts])` | `request(addr, msg).await` / `request_with(addr, msg, opts).await` | ✅ 默认 30s,DeliveryOptions 对齐 |
| EventBus.consumer | `MessageConsumer<T>.handler(h)` 推模型 | `bus.consumer::<T>(addr)` 既可 `.recv().await` 拉,也可 `.handler(fn)` 推 | ✅ 两种风格并存 |
| Verticle 撤销自动清扫 | Vert.x undeploy 自动注销该 verticle 的 consumer | bound EventBus + DeploymentScope.cleanup | ✅ |
| Timer (one-shot) | `vertx.timer(delay)` 返回 `Timer extends Future<Void>` | `Vertx::timer(d)` 返回 `Timer: Future<Output=Result<()>>` | ✅ |
| Timer.cancel | `Timer.cancel()` | `Timer::cancel()` | ✅ |
| Periodic | `vertx.setPeriodic(initial, period, handler)` | `Vertx::periodic(initial, period)` + `.tick().await` | Rust 拉模型 |
| Timer 绑定 Verticle | Vert.x 撤销 verticle 时其注册的 timer 自动取消 | `Context::timer / periodic` 跟 scope 联动 | ✅ |

### 10.2 故意未对齐 / MVP 不做

| 能力 | Vert.x | 我们的选择 | 原因 |
|---|---|---|---|
| Verticle 生命周期形态 | `start(Promise<Void>)` + `stop(Promise<Void>)` 两钩,实例由框架持有 | **owned-self `run(self, ctx) -> Result<()>`**,实例 = task | Rust 所有权模型下 owned-self 更自然 —— state 用普通字段(`Cell`、`HashMap`)无需 Rc/RefCell;清理在 fn 末尾,无外部编排 |
| 用户指定 EventLoop | `DeploymentOptions::setInstances` + 内部分配 | 不暴露,RR 框架内部分派 | 亲和性是运行时不变量 |
| Verticle 跨 EventLoop 启动 task | 通过 `runOnContext(otherCtx)` 间接支持 | 不支持 | 保持亲和性语义清晰 |
| 老 Timer API | `setTimer(delay, handler) -> long id` + `cancelTimer(id)` | 不提供 | Rust 回调风格不自然,`Timer::cancel` 已覆盖能力 |
| `executeBlocking(ordered)` | `ordered=true` 同 context 多次调用串行 | 当前全 unordered | 后续可选添加 |
| `DeliveryOptions` | timeout / headers / codec / localOnly | timeout 已实现;headers 在 Envelope 上;codec/localOnly 暂未实现 | MVP 先 timeout,后续在 DeliveryOptions 内扩展 |
| `MessageCodec` | 注册编解码器 | 类型擦除走 `Box<dyn Any>` | 集群阶段引入 serde |
| `WorkerExecutor` 命名池 | `createSharedWorkerExecutor(name, size)` | 仅一个全局 worker pool | 后续可加 |
| `Context.put/get/remove` | 每 Context 的 KV 存储 | 不提供 | Rust 类型擦除别扭 |
| `VIRTUAL_THREAD` | Java 21 虚拟线程 verticle | 不模型化 | Tokio task 已经满足 |
| `EXTERNAL` Context | 非 Vertx 线程的隐式 Context | `Context::current()` 返回 None | 我们不替外部线程"伪造"Context |
| 集群 / 分布式 EventBus | Hazelcast / Infinispan 等 | 不做 | 范围外 |
| HTTP / TCP / WebSocket / FileSystem | 一整套 IO 抽象 | 不做 | 范围外,可作为独立 crate |

### 10.3 命名差异

| Vert.x | vertx-rs | 备注 |
|---|---|---|
| `vertx.timer(delay)` | `vx.timer(delay)` | 简写一致 |
| `vertx.setPeriodic(initial, period, h)` | `vx.periodic(initial, period)` + `tick().await` | 拉模型 |
| `Vertx.currentContext()` | `Context::current()` | 静态方法位置不同 |
| `vertx.eventBus()` | `vx.event_bus()` | snake_case |
| `MessageConsumer<T>.handler(h)` | `Consumer<T>.recv().await` | 推 vs 拉 |
