# Sentinel 1.8.10 滑动窗口实现原理深度分析

> 基于 Sentinel 1.8.10 源码，源码根目录：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/`
> 核心包：`slots/statistic/base/` 与 `slots/statistic/metric/`

## 一、整体概览

Sentinel 的实时统计（QPS、RT、异常数、拦截数等）全部建立在**滑动窗口（Sliding Window）**算法之上。其核心数据结构是 `LeapArray`（跳跃数组）——一个用**环形数组 + 时间分桶**实现的滑动窗口。

调用链路（自顶向下）：

```
StatisticNode
   └── ArrayMetric (Metric 接口实现，对外暴露 success/pass/block/rt 等聚合方法)
          └── LeapArray<MetricBucket> (滑动窗口骨架，环形数组 + CAS/锁)
                 ├── BucketLeapArray          (普通分桶实现)
                 └── OccupiableBucketLeapArray(支持"借用"未来桶的实现)
                        └── FutureBucketLeapArray (借用的"未来桶"数组)

WindowWrap<T>     (单个窗口桶的包装：起始时间 + 窗口长度 + 统计值)
MetricBucket      (桶内统计值：LongAdder[] 计数器 + minRt)
TimeUtil          (高性能时间源，驱动窗口滚动)
```

默认配置（来自 `SampleCountProperty` / `IntervalProperty` / `RuleConstant`）：

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `INTERVAL` | `1000 ms` | 滑动窗口总跨度 |
| `SAMPLE_COUNT` | `2` | 桶数量（样本数） |
| `windowLengthInMs` | `INTERVAL / SAMPLE_COUNT = 500 ms` | 单个桶的时间跨度 |

即默认情况下：**1 秒的滑动窗口被切成 2 个 500ms 的桶**，环形数组长度为 2。

---

## 二、核心类解析

### 2.1 `WindowWrap<T>` —— 单个窗口桶

> 源码：`slots/statistic/base/WindowWrap.java`

`WindowWrap` 是一个时间桶的包装实体，泛型 `T` 是桶内实际的统计值类型（如 `MetricBucket`）。

```java
public class WindowWrap<T> {
    private final long windowLengthInMs;   // 单个桶的时间长度（不可变）
    private long windowStart;              // 桶的起始时间戳（可变，桶复用时会被 reset）
    private T value;                       // 桶内的统计数据
    ...
}
```

关键方法：

- `resetTo(long startTime)`：把桶的起始时间重置为指定时刻（复用旧桶时调用，**不重置 value**，value 的清理由子类 `resetWindowTo` 完成）。
- `isTimeInWindow(long timeMillis)`：判断某个时间戳是否落在本桶内
  ```java
  return windowStart <= timeMillis && timeMillis < windowStart + windowLengthInMs;
  ```
  注意是**左闭右开**区间 `[windowStart, windowStart + windowLengthInMs)`。

`WindowWrap` 本身只是个"壳"，真正存数据的是 `value`（`MetricBucket`），而数组里存的是 `WindowWrap` 引用。

### 2.2 `LeapArray<T>` —— 滑动窗口骨架

> 源码：`slots/statistic/base/LeapArray.java`

这是整个滑动窗口的**核心抽象类**，所有窗口实现都继承自它。

#### 2.2.1 核心字段

```java
protected int windowLengthInMs;              // 单桶时长 = intervalInMs / sampleCount
protected int sampleCount;                   // 桶数量（= 数组长度）
protected int intervalInMs;                  // 窗口总跨度
private double intervalInSecond;             // 窗口总跨度（秒）

protected final AtomicReferenceArray<WindowWrap<T>> array;  // 环形数组！
private final ReentrantLock updateLock = new ReentrantLock(); // 条件更新锁
```

最关键的是 `array`——一个 **`AtomicReferenceArray`**，长度 = `sampleCount`。它是"环形数组"的载体：通过**取模下标**实现循环复用，无需移动元素。

#### 2.2.2 构造与校验

```java
public LeapArray(int sampleCount, int intervalInMs) {
    AssertUtil.isTrue(sampleCount > 0, ...);
    AssertUtil.isTrue(intervalInMs > 0, ...);
    AssertUtil.isTrue(intervalInMs % sampleCount == 0, "time span needs to be evenly divided");
    this.windowLengthInMs = intervalInMs / sampleCount;
    ...
    this.array = new AtomicReferenceArray<>(sampleCount);
}
```

三条强约束：
1. `sampleCount > 0`；
2. `intervalInMs > 0`；
3. **`intervalInMs` 必须能被 `sampleCount` 整除**——保证每个桶时长是整数毫秒，时间分桶才能对齐。

#### 2.2.3 两个基础计算函数（理解一切的关键）

```java
private int calculateTimeIdx(long timeMillis) {
    long timeId = timeMillis / windowLengthInMs;   // 当前时间属于第几个"桶周期"
    return (int)(timeId % array.length());          // 对数组长度取模 → 环形下标
}

protected long calculateWindowStart(long timeMillis) {
    return timeMillis - timeMillis % windowLengthInMs;  // 桶起始时间 = 时间戳对齐到桶边界
}
```

**理解这两个函数是理解整个滑动窗口的关键。**

- `calculateTimeIdx`：把绝对时间映射到环形数组的下标。因为 `timeId` 随时间单调递增，对数组长度取模后，下标会**周期性循环**——这就是"环形"复用的本质。同一时刻只用一个桶，过了一个 `windowLengthInMs` 就推进到下一个下标。
- `calculateWindowStart`：把任意时间戳**向下取整**到桶的起始边界。例如 `windowLengthInMs=500`，时间戳 `888` 的 `windowStart = 888 - 888%500 = 500`。

以默认配置（`windowLengthInMs=500`, `sampleCount=2`）为例：

```
时间轴(ms): 0    500   1000   1500   2000   2500
桶周期timeId: 0    1     2      3      4      5
数组下标:    0    1     0      1      0      1   ← (timeId % 2)
windowStart: 0    500   1000   1500   2000   2500
```

可见下标 `0` 在 `0~500`、`1000~1500`、`2000~2500` 期间被复用——这正是"环形"复用，**数组元素会被反复重置覆盖**，所以滑动窗口内存占用恒定（O(sampleCount)），与时间无关。

#### 2.2.4 抽象方法（由子类实现）

```java
public abstract T newEmptyBucket(long timeMillis);                       // 创建空桶
protected abstract WindowWrap<T> resetWindowTo(WindowWrap<T> w, long startTime); // 复用旧桶时重置
```

这两个方法把"如何创建/重置统计值"延迟到子类，使 `LeapArray` 与具体统计类型解耦。

#### 2.2.5 ⭐ `currentWindow(long timeMillis)` —— 核心方法

这是滑动窗口最核心、最精妙的方法，所有写入统计数据的入口（`addPass`、`addSuccess` 等）都先调用它拿到当前桶。

```java
public WindowWrap<T> currentWindow(long timeMillis) {
    if (timeMillis < 0) return null;

    int idx = calculateTimeIdx(timeMillis);
    long windowStart = calculateWindowStart(timeMillis);

    while (true) {
        WindowWrap<T> old = array.get(idx);
        if (old == null) {
            // (1) 桶不存在 → 新建 + CAS
            WindowWrap<T> window = new WindowWrap<>(windowLengthInMs, windowStart, newEmptyBucket(timeMillis));
            if (array.compareAndSet(idx, null, window)) {
                return window;            // CAS 成功
            } else {
                Thread.yield();           // CAS 失败，让出 CPU 重试
            }
        } else if (windowStart == old.windowStart()) {
            // (2) 桶是当前窗口 → 直接返回
            return old;
        } else if (windowStart > old.windowStart()) {
            // (3) 桶已过期 → 加锁重置（复用旧桶）
            if (updateLock.tryLock()) {
                try {
                    return resetWindowTo(old, windowStart);
                } finally {
                    updateLock.unlock();
                }
            } else {
                Thread.yield();           // 抢锁失败，让出 CPU 重试
            }
        } else if (windowStart < old.windowStart()) {
            // (4) 时钟回退 → 理论不应发生，返回新建的临时桶
            return new WindowWrap<>(windowLengthInMs, windowStart, newEmptyBucket(timeMillis));
        }
    }
}
```

该方法用 `while(true)` 自旋处理**四种情况**，对应源码注释中的 ASCII 图：

**情况 (1)：桶不存在（`old == null`）**

```
    B0       B1      B2    NULL      B4
||_______|_______|_______|_______|_______||___
200     400     600     800     1000    1200
                          ^
                       time=888  (idx 处为 null)
```
数组该位置还没初始化过（或被 GC？实际不会，因为引用一直存在，只是首次写入）。新建 `WindowWrap`，用 **CAS** `compareAndSet(idx, null, window)` 写入。CAS 保证只有一个线程成功创建，其他线程 `Thread.yield()` 后重试。重试时 `old` 已不为 null，会进入情况 (2)。

**情况 (2)：桶是当前窗口（`windowStart == old.windowStart()`）**

```
    B0       B1      B2     B3      B4
||_______|_______|_______|_______|_______||___
200     400     600     800     1000    1200
                          ^
                       time=888, 桶 B3 起始 800 → 命中
```
请求时间落在旧桶的时间范围内，桶仍是当前有效窗口，**直接返回**，无需任何同步——这是最常见的快路径，性能极高。

**情况 (3)：桶已过期（`windowStart > old.windowStart()`）**

```
  (old)          B0       B1      B2    NULL      B4
|_______||_______|_______|_______|_______|_______||___
...   1200     1400    1600    1800    2000    2200
                          ^
                       time=1676, 该 idx 处旧桶起始 400 → 已过期
```
旧桶的起始时间已经落后于当前 `windowStart`（说明窗口已经滚过该桶至少一圈），需要**复用旧桶**：重置 `windowStart` 并清空 `value`。由于"重置起始时间 + 清空计数器"不是原子操作，这里用 `updateLock.tryLock()` 串行化。抢到锁的线程执行 `resetWindowTo`，没抢到的线程 `yield` 后重试（重试时会命中情况 (2)）。

> **为什么 CAS 而不用锁？为什么过期重置用锁而不用 CAS？**
> - 情况 (1) 是"从 null 到对象"的指针置换，CAS 足够且无锁性能最好；
> - 情况 (3) 需要修改对象内部状态（`windowStart` + `MetricBucket` 多个计数器），无法用单一 CAS 表达，故用 `ReentrantLock`。该锁是**条件锁（仅在桶过期时才抢）**，作用域极小，正常热路径不触发，所以"大多数情况下不会带来性能损失"（源码注释原话）。

**情况 (4)：时钟回退（`windowStart < old.windowStart()`）**

提供的 `timeMillis` 比旧桶还旧，理论上不应发生（时钟回拨）。源码选择不写入共享数组，而是**返回一个临时新建的桶**，避免污染共享状态。注意：写入这个临时桶的统计数据会丢失，这是 Sentinel 对时钟回退的"安全丢弃"策略。

#### 2.2.6 过期判定：`isWindowDeprecated`

```java
public boolean isWindowDeprecated(long time, WindowWrap<T> windowWrap) {
    return time - windowWrap.windowStart() > intervalInMs;
}
```

一个桶"过期"的判定标准是：**当前时间距该桶起始时间已超过一个完整的窗口跨度 `intervalInMs`**。以默认配置（`intervalInMs=1000`）为例，桶起始 `500`，当前 `1550`，则 `1550 - 500 = 1050 > 1000` → 过期。

注意：过期判定是**逻辑层面**的（基于时间差），不是物理删除。过期桶仍留在数组里，只是在聚合统计时被跳过，等环形复用到该下标时才被 `resetWindowTo` 物理重置。

#### 2.2.7 聚合查询方法

`LeapArray` 提供多种遍历方法，全部基于"过滤过期桶"：

```java
public List<WindowWrap<T>> list(long validTime) {       // 有效桶列表
    for (int i = 0; i < size; i++) {
        WindowWrap<T> w = array.get(i);
        if (w == null || isWindowDeprecated(validTime, w)) continue;
        result.add(w);
    }
}

public List<T> values(long timeMillis) { ... }          // 有效桶的 value 列表
public List<WindowWrap<T>> listAll() { ... }            // 所有桶（含过期），用于 debug
```

聚合统计（如 QPS）就是遍历 `values()` 累加。**这就是"滑动窗口"的滑动性来源**：每次查询时实时计算"当前 `intervalInMs` 内有效桶的总和"，随着时间推进，旧桶自然过期被剔除，新桶自然加入——窗口就像在时间轴上"滑动"。

#### 2.2.8 `getPreviousWindow` —— 前一个窗口

```java
public WindowWrap<T> getPreviousWindow(long timeMillis) {
    int idx = calculateTimeIdx(timeMillis - windowLengthInMs);  // 往前推一个桶
    timeMillis = timeMillis - windowLengthInMs;
    WindowWrap<T> wrap = array.get(idx);
    if (wrap == null || isWindowDeprecated(wrap)) return null;
    if (wrap.windowStart() + windowLengthInMs < (timeMillis)) return null;  // 边界再校验
    return wrap;
}
```

用于获取"上一个桶"的统计（如 `previousWindowPass`、`previousWindowBlock`），在某些限流算法（如预热、匀速）中需要参考上一窗口数据。

---

### 2.3 `MetricBucket` —— 桶内统计值

> 源码：`slots/statistic/data/MetricBucket.java`

```java
public class MetricBucket {
    private final LongAdder[] counters;   // 按 MetricEvent 枚举序号索引
    private volatile long minRt;
    ...
}
```

- 用 **`LongAdder` 数组**而非 `AtomicLong`。`LongAdder` 在高并发写时通过 Cell 分段累加，写性能远高于 `AtomicLong`（牺牲一点读的 `sum()` 一致性），非常适合 Sentinel 这种"写多读少"的统计场景。
- `counters` 按 `MetricEvent` 枚举的 `ordinal()` 索引，事件类型有 `PASS`、`BLOCK`、`EXCEPTION`、`SUCCESS`、`RT`、`OCCUPIED_PASS` 等。
- `minRt`（最小响应时间）用 `volatile long`，更新时注释明确写道 "Not thread-safe, but it's okay"——容忍极小误差换取性能。
- `reset()` 清零所有计数器；`reset(MetricBucket b)` 清零后把 `b` 的值拷过来（供占用机制使用）。

---

### 2.4 `BucketLeapArray` —— 普通分桶实现

> 源码：`slots/statistic/metric/BucketLeapArray.java`

最基础的实现，泛型具化为 `MetricBucket`：

```java
public class BucketLeapArray extends LeapArray<MetricBucket> {
    public MetricBucket newEmptyBucket(long time) { return new MetricBucket(); }

    protected WindowWrap<MetricBucket> resetWindowTo(WindowWrap<MetricBucket> w, long startTime) {
        w.resetTo(startTime);   // 重置起始时间
        w.value().reset();      // 清空计数器
        return w;
    }
}
```

复用旧桶时：先把 `windowStart` 重置为当前时间对齐值，再把 `MetricBucket` 内所有计数器清零。简单直接。

---

### 2.5 `ArrayMetric` —— 对外统计门面

> 源码：`slots/statistic/metric/ArrayMetric.java`

`ArrayMetric` 实现 `Metric` 接口，内部持有一个 `LeapArray<MetricBucket> data`，是 `StatisticNode` 直接使用的统计门面。

构造器决定底层用哪种 LeapArray：

```java
public ArrayMetric(int sampleCount, int intervalInMs) {
    this.data = new OccupiableBucketLeapArray(sampleCount, intervalInMs);  // 默认开启占用
}
public ArrayMetric(int sampleCount, int intervalInMs, boolean enableOccupy) {
    this.data = enableOccupy ? new OccupiableBucketLeapArray(...)
                             : new BucketLeapArray(...);
}
```

> 注意：默认构造（两参）**强制使用 `OccupiableBucketLeapArray`**，即默认开启"未来桶占用"能力。

**写入**（每次请求都会调用）：

```java
public void addPass(int count) {
    WindowWrap<MetricBucket> wrap = data.currentWindow();  // ← 先拿到/创建当前桶
    wrap.value().addPass(count);                            // ← 再往桶里累加
}
```

所有 `addXxx` 方法都是这两步：`currentWindow()` 定位桶 + `value().addXxx()` 累加。

**聚合读取**（如 QPS）：

```java
public long pass() {
    data.currentWindow();              // 触发当前桶的更新（懒滚动）
    long pass = 0;
    for (MetricBucket window : data.values()) {  // 遍历所有有效桶
        pass += window.pass();
    }
    return pass;
}
```

关键点：**每次读之前先调用一次 `data.currentWindow()`**。这有两层含义：
1. 确保当前时间对应的桶已被创建/滚动到位（懒触发窗口推进）；
2. 之后再 `values()` 遍历有效桶累加——过期桶被 `isWindowDeprecated` 过滤。

`getAvg(event)` 把总和除以 `intervalInSecond` 得到每秒均值（QPS 即由此而来）。

---

### 2.6 `OccupiableBucketLeapArray` 与"未来桶占用"

> 源码：`slots/statistic/metric/occupy/OccupiableBucketLeapArray.java`、`FutureBucketLeapArray.java`

这是 Sentinel 滑动窗口的一个进阶能力：**提前"借用"未来的配额**（用于"占用式通过"，即当前已超限但允许占用未来窗口的额度，类似令牌桶的透支）。

#### 2.6.1 结构：双数组

```java
public class OccupiableBucketLeapArray extends LeapArray<MetricBucket> {
    private final FutureBucketLeapArray borrowArray;  // 借用数组（记录未来桶的预占）
    ...
}
```

`OccupiableBucketLeapArray` 自身是"主数组"（记录已发生统计），`borrowArray` 是"借用数组"（记录对未来桶的预占）。两者结构相同、时间对齐。

#### 2.6.2 `FutureBucketLeapArray` 的"小技巧"

```java
public class FutureBucketLeapArray extends LeapArray<MetricBucket> {
    @Override
    public boolean isWindowDeprecated(long time, WindowWrap<MetricBucket> windowWrap) {
        // Tricky: will only calculate for future.
        return time >= windowWrap.windowStart();   // ← 关键：时间到了桶起始时刻就算"过期"
    }
}
```

借用数组的 `isWindowDeprecated` 被重写：当真实时间推进到某个未来桶的起始时刻时，该桶就"过期"了——因为它从"未来"变成了"现在/过去"，应当被迁移到主数组。

#### 2.6.3 占用如何与主数组合并

`newEmptyBucket` 和 `resetWindowTo` 在创建/重置主数组桶时，会去借用数组里查"这个时刻有没有被预占过"，若有则把预占的 pass 值迁移过来：

```java
public MetricBucket newEmptyBucket(long time) {
    MetricBucket newBucket = new MetricBucket();
    MetricBucket borrowBucket = borrowArray.getWindowValue(time);
    if (borrowBucket != null) {
        newBucket.reset(borrowBucket);   // 把借用桶的值拷到新桶
    }
    return newBucket;
}

protected WindowWrap<MetricBucket> resetWindowTo(WindowWrap<MetricBucket> w, long time) {
    w.resetTo(time);
    MetricBucket borrowBucket = borrowArray.getWindowValue(time);
    if (borrowBucket != null) {
        w.value().reset();
        w.value().addPass((int) borrowBucket.pass());  // 预占的 pass 计入新桶
    } else {
        w.value().reset();
    }
    return w;
}
```

`addWaiting(time, count)` 把预占写入借用数组的对应未来桶：

```java
public void addWaiting(long time, int acquireCount) {
    WindowWrap<MetricBucket> window = borrowArray.currentWindow(time);
    window.value().add(MetricEvent.PASS, acquireCount);
}
```

`currentWaiting()` 汇总借用数组里所有未来桶的预占总量，供限流判断"未来已被占了多少"。

**整体效果**：当时间推进到某个曾经被预占的"未来桶"时，主数组通过 `currentWindow` 滚动到该桶（创建或重置），此时 `newEmptyBucket`/`resetWindowTo` 自动把借用数组里记录的预占值迁移到主数组，于是历史预占"兑现"为正式统计。借用数组那边则因 `isWindowDeprecated` 重写而把该桶视为过期。两个数组协同完成了"先借后还"的透支记账。

---

### 2.7 `TimeUtil` —— 高性能时间源

> 源码：`util/TimeUtil.java`

滑动窗口的所有时间戳来自 `TimeUtil.currentTimeMillis()`，而非每次 `System.currentTimeMillis()`。这是一个**自适应高性能时钟**，值得单独说明：

- 后台守护线程 `sentinel-time-tick-thread` 维护一个 `volatile long currentTimeMillis` 字段。
- **空闲态（IDLE）**：每 300ms sleep 一次，`currentTimeMillis()` 调用时直接读字段（读时若发现状态非 RUNNING 则顺便用 `System.currentTimeMillis()` 刷新并回填）。
- **繁忙态（RUNNING）**：每 1ms 用 `System.currentTimeMillis()` 刷新字段——此时 `currentTimeMillis()` 退化为纯字段读，避免高并发下频繁系统调用。
- 通过自身的 `LeapArray<Statistic>` 统计读/写 QPS，每 3 秒检查一次：QPS 超过 1200/s 切到 PREPARE→RUNNING，低于 800/s 切回 IDLE。

设计动机：Sentinel 每个请求都要调用多次 `currentTimeMillis()`（如 `currentWindow`），高并发下 `System.currentTimeMillis()` 的系统调用开销不可忽视，用内存字段读替代可显著降低开销（详见源码引用的 issue #1702）。

---

## 三、滑动窗口工作流程图解

以默认配置（`intervalInMs=1000`，`sampleCount=2`，`windowLengthInMs=500`）为例。

### 3.1 环形数组滚动示意

```
数组: array[0], array[1]   (长度 2)

时间 t=100ms:
  timeId = 100/500 = 0,  idx = 0%2 = 0,  windowStart = 100-100%500 = 0
  array[0] = WindowWrap(start=0,    value=MetricBucket{pass=1})
  array[1] = null

时间 t=600ms:
  timeId = 600/500 = 1,  idx = 1%2 = 1,  windowStart = 600-600%500 = 500
  array[0] = WindowWrap(start=0,    ...)   ← 仍有效
  array[1] = WindowWrap(start=500,  ...)   ← 新建 (情况1 CAS)

时间 t=1100ms:
  timeId = 1100/500 = 2,  idx = 2%2 = 0,  windowStart = 1100-100 = 1000
  array[0] 旧值 start=0, 而 windowStart=1000 > 0 → 过期 (情况3)
            → 加锁 resetWindowTo: start 重置为 1000, value 清零
  array[1] = WindowWrap(start=500, ...)    ← 仍有效 (1000-500=500 ≤ 1000, 不过期)

时间 t=1600ms:
  timeId = 1600/500 = 3,  idx = 3%2 = 1,  windowStart = 1500
  array[1] 旧值 start=500, windowStart=1500 > 500 → 过期 → 重置为 1500
```

可见下标 `0` 和 `1` 交替复用，**数组长度恒为 2，但覆盖了任意 1 秒窗口**。

### 3.2 聚合查询：窗口"滑动"的本质

某时刻 `t=1250ms` 调用 `pass()`：

1. `currentWindow(1250)`：`idx=0`, `windowStart=1000`。若 `array[0]` 当前是 `start=1000`，命中情况 (2) 直接返回；若仍是上一轮旧桶则触发情况 (3) 重置。
2. `values(1250)` 遍历两个桶，用 `isWindowDeprecated(1250, w)` 过滤：
   - `array[0]` start=1000：`1250-1000=250 ≤ 1000` → 有效 ✅
   - `array[1]` start=500：`1250-500=750 ≤ 1000` → 有效 ✅
   - （若 `array[1]` start=0：`1250-0=1250 > 1000` → 过期 ❌ 被剔除）
3. 累加有效桶的 pass 即得最近 1 秒的通过量。

**"滑动"就体现在第 2 步**：随着 `t` 增大，旧桶逐步跨越 `intervalInMs` 阈值被判为过期而从聚合中"滑出"，新桶则因被写入而"滑入"，窗口内容随时间平滑更替，且无需任何元素搬移。

### 3.3 完整请求统计时序

```
请求到达
   │
   ▼
StatisticNode.addPassRequest / addRtAndSuccess ...
   │
   ▼
ArrayMetric.addPass(count)  →  data.currentWindow()  ──┐
                                                        │
                              ┌─────────────────────────┘
                              ▼
                   LeapArray.currentWindow(timeMillis)
                              │
              ┌───────────────┼───────────────┬────────────────┐
              ▼               ▼               ▼                ▼
        old==null        命中当前窗口      桶过期           时钟回退
        CAS 新建         直接返回         tryLock 重置      返回临时桶
              │               │               │
              └───────────────┴───────────────┘
                              │
                              ▼
                     返回 WindowWrap<MetricBucket>
                              │
                              ▼
              wrap.value().addPass(count)  →  LongAdder.add(count)
```

限流判断时（如 `StatisticNode.passQps()`）：

```
passQps() = rollingCounterInSecond.pass() / intervalInSecond
                │
                ▼
        ArrayMetric.pass()
                │
        ┌───────┴───────┐
        ▼               ▼
 data.currentWindow()  遍历 data.values() 累加
 (懒触发滚动)          (过滤过期桶)
```

---

## 四、并发与正确性分析

Sentinel 滑动窗口要在**极高并发**下保证统计正确，其并发设计是精髓：

### 4.1 写入路径的并发控制

| 场景 | 同步机制 | 说明 |
|------|----------|------|
| 首次建桶（null→obj） | `AtomicReferenceArray.compareAndSet` | 无锁 CAS，仅一个线程成功 |
| 命中当前桶 | **无任何同步** | 直接返回引用，最高频快路径 |
| 桶过期复用 | `ReentrantLock.tryLock` | 串行化重置，失败者 yield 重试 |
| 时钟回退 | 无 | 返回临时桶，丢弃写入 |

### 4.2 计数器并发

`MetricBucket` 内部用 `LongAdder[]`，写用 `add`（分段 Cell 累加，几乎无竞争），读用 `sum()`（汇总各 Cell，最终一致）。这保证了高并发写计数器本身不会成为瓶颈。

### 4.3 可见性

- `array` 是 `AtomicReferenceArray`，其 `get`/`compareAndSet` 提供 volatile 语义，保证桶引用的发布与可见性。
- `WindowWrap.windowStart` 虽然是非 volatile 的 `long`，但其修改发生在 `updateLock` 临界区内（过期重置路径），且 `windowStart == old.windowStart()` 的相等比较在大多数 JIT/平台下对 long 读是原子的；Sentinel 在此**牺牲了极小概率的可见性延迟以换取性能**，对统计这种"近似值"场景可接受。
- `MetricBucket.minRt` 显式标明 "Not thread-safe, but it's okay"——同样是工程上的权衡。

### 4.4 为何过期重置用锁而非 CAS？

桶过期复用要做两件事：(a) `windowWrap.resetTo(windowStart)` 改起始时间；(b) `value.reset()` 清空多个 `LongAdder`。这是复合操作，无法用一个 CAS 完成；若用 CAS 重试则需要在循环里反复创建新 `MetricBucket`，开销大且会产生垃圾对象。用 `ReentrantLock` 串行化最自然。又因该路径只在桶过期（一个桶约 500ms 才发生一次）时触发，锁竞争极低，故源码称"大多数情况下不会带来性能损失"。

### 4.5 `Thread.yield()` 的考量

CAS/抢锁失败时不是阻塞（`wait`/`park`），而是 `Thread.yield()` 后立即重试。因为临界区极短（几次字段写），让出 CPU 时间片后对方很快完成，自旋重试比阻塞-唤醒开销更小。

---

## 五、设计亮点与小结

1. **环形数组复用，内存恒定**：用 `timeMillis / windowLengthInMs % length` 把时间映射到固定下标，数组长度恒为 `sampleCount`，无 GC 压力，无元素搬移。
2. **快路径无锁**：命中当前桶（最高频）完全无同步，性能瓶颈只在首次建桶（CAS）和过期重置（条件锁，低频）。
3. **CAS + 条件锁的精准配合**：能 CAS 的用 CAS，必须复合操作的用小作用域锁，二者各司其职。
4. **`LongAdder` 替代 `AtomicLong`**：高并发写计数器的最优解。
5. **懒滚动**：窗口推进不是定时器驱动，而是"读写时按需触发"——`currentWindow()` 在读写前被调用，顺带完成过期桶的重置，无后台线程开销（统计层面）。
6. **逻辑过期而非物理删除**：过期桶仍占位，靠 `isWindowDeprecated` 过滤，简化了并发控制。
7. **双数组实现"未来占用"**：`OccupiableBucketLeapArray` + `FutureBucketLeapArray` 用对称的两个滑动窗口实现透支记账，优雅支持令牌桶式预占。
8. **自适应时钟 `TimeUtil`**：根据负载在"系统调用"与"内存读"间切换，从时间源头降低开销。

### 核心源码索引

| 文件 | 职责 |
|------|------|
| `slots/statistic/base/LeapArray.java` | 滑动窗口骨架：环形数组、`currentWindow`、过期判定、聚合遍历 |
| `slots/statistic/base/WindowWrap.java` | 单个窗口桶包装（起始时间 + 长度 + 统计值） |
| `slots/statistic/base/UnaryLeapArray.java` | 泛型为 `LongAdder` 的简单实现 |
| `slots/statistic/metric/BucketLeapArray.java` | 泛型为 `MetricBucket` 的基础实现 |
| `slots/statistic/metric/ArrayMetric.java` | `Metric` 门面，封装写入与聚合查询 |
| `slots/statistic/metric/occupy/OccupiableBucketLeapArray.java` | 支持"未来桶占用"的实现 |
| `slots/statistic/metric/occupy/FutureBucketLeapArray.java` | 借用数组，重写过期判定 |
| `slots/statistic/data/MetricBucket.java` | 桶内 `LongAdder[]` 计数器 |
| `util/TimeUtil.java` | 自适应高性能时钟 |
| `node/StatisticNode.java` | 统计节点，持有秒级/分钟级两个 `ArrayMetric` |
| `node/SampleCountProperty.java` / `IntervalProperty.java` | 桶数与窗口跨度配置（默认 2 / 1000ms） |

### 一句话总结

> Sentinel 的滑动窗口 = **固定长度的 `AtomicReferenceArray` 环形数组**（按 `时间/桶长 % 长度` 定位下标）+ **每个桶 `WindowWrap<MetricBucket>`**（`LongAdder[]` 计数）+ **`currentWindow` 四态自旋**（null→CAS、命中→直返、过期→条件锁重置、回退→丢弃）+ **逻辑过期过滤**实现的时间聚合。窗口的"滑动"不是数据搬移，而是查询时按 `intervalInMs` 实时过滤有效桶的累加结果。