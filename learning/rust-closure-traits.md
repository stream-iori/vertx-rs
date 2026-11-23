# Rust 闭包的 Fn、FnMut 和 FnOnce

给函数参数选择闭包 trait 时，先看调用方需要什么能力。只调用一次就用 `FnOnce`，需要反复调用并允许修改捕获变量就用 `FnMut`，需要通过共享引用反复调用就用 `Fn`。

| Trait | 调用时如何使用闭包 | 典型行为 |
|---|---|---|
| `Fn` | 共享借用 `&self` | 每次读取捕获的配置。也可以通过 `Cell`、原子类型等修改内部状态。 |
| `FnMut` | 可变借用 `&mut self` | 每次修改闭包捕获的计数器。 |
| `FnOnce` | 取得所有权 `self` | 把捕获的值移出去，或把它交给另一个闭包。 |

`Fn` 也满足 `FnMut`，`FnMut` 也满足 `FnOnce`。所以参数写 `F: FnOnce(...)` 时，普通函数和可重复调用的闭包也能传入。

`move` 决定创建闭包时如何捕获变量。能否重复调用，要看执行闭包体时如何使用捕获值。

## 完整对比

```rust
fn main() {
    let name = String::from("Alice");
    let greet = move || println!("你好，{name}"); // Fn
    greet();
    greet();

    let mut count = 0;
    let mut next = move || {                    // FnMut
        count += 1;
        count
    };
    assert_eq!(next(), 1);
    assert_eq!(next(), 2);

    let token = String::from("ready");
    let take_token = move || token;             // FnOnce
    assert_eq!(take_token(), "ready");
    // take_token(); // 编译错误，第一次调用已消费闭包
}
```

## vertx-rs 为什么用 FnOnce

`build_with_pools()` 接收建池函数。正常构建传入 `EventLoopPool::new` 和 `WorkerPool::new`，测试可以传入会失败的工厂，验证已启动的线程是否被回收。

```rust
E: FnOnce(usize, u64) -> Result<EventLoopPool>,
```

两个参数分别是线程数和 Vertx 实例 ID。构建器最多调用一次 `event_loops(el, instance)`，一次就创建整个池。池再按线程数启动各个 EventLoop。[构建器与调用点](../src/vertx.rs#L639) · [测试中的工厂](../src/vertx.rs#L733)

测试把一个共享的 `probe` 交给池内部的初始化闭包。下面是这个所有权关系的可运行简化例子：

```rust
use std::sync::Arc;

fn main() {
    let probe = Arc::new(String::from("probe"));

    let fn_factory = {
        let probe = Arc::clone(&probe);
        move || {
            let task_probe = Arc::clone(&probe); // 工厂保留自己的那份 Arc
            move || println!("{task_probe}")
        }
    };
    fn_factory()();
    fn_factory()(); // 工厂仍可再次调用

    let once_factory = move || {
        move || println!("{probe}") // 把工厂持有的 Arc 直接移进任务
    };
    let task = once_factory();
    task();
    task(); // 内层任务只是读取 probe，可以反复调用
    // once_factory(); // 工厂已被消费
}
```
