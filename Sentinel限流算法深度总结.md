# Sentinel 1.8.10 限流算法深度总结

> 基于 Sentinel 1.8.10 源码。限流（Flow Control）相关代码集中在 `slots/block/flow/`，熔断降级在 `slots/block/degrade/circuitbreaker/`，系统自适应在 `slots/system/`。
> 本文逐一拆解每种算法的原理、源码实现、参数与适用场景。

---

## 〇、总体架构：算法如何被选中与调用

Sentinel 的限流算法统一抽象为接口 `TrafficShapingController`（流量整形控制器）：

```java
// slots/block/flow/TrafficShapingController.java
public interface TrafficShapingController {
    boolean canPass(Node node, int acquireCount, boolean prioritized);
    boolean canPass(Node node, int acquireCount);
}
```

每条 `FlowRule` 有一个 `controlBehavior` 字段，决定使用哪种控制器。路由逻辑在 `FlowRuleUtil.generateRater(FlowRule rule)`：

```java
// slots/block/flow/FlowRuleUtil.java
private static TrafficShapingController generateRater(FlowRule rule) {
    if (rule.getGrade() == RuleConstant.FLOW_GRADE_QPS) {
        switch (rule.getControlBehavior()) {
            case CONTROL_BEHAVIOR_WARM_UP:               // 1
                return new WarmUpController(rule.getCount(), rule.getWarmUpPeriodSec(), ColdFactorProperty.coldFactor);
            case CONTROL_BEHAVIOR_RATE_LIMITER:          // 2
                return new ThrottlingController(rule.getMaxQueueingTimeMs(), rule.getCount());
            case CONTROL_BEHAVIOR_WARM_UP_RATE_LIMITER:  // 3
                return new WarmUpRateLimiterController(rule.getCount(), rule.getWarmUpPeriodSec(),
                        rule.getMaxQueueingTimeMs(), ColdFactorProperty.coldFactor);
            case CONTROL_BEHAVIOR_DEFAULT:               // 0
            default:
                // 默认/未知：快速失败
        }
    }
    return new DefaultController(rule.getCount(), rule.getGrade());
}
```

四种 `controlBehavior` 常量（`slots/block/RuleConstant.java`）：

| 常量 | 值 | 控制器 | 算法本质 |
|------|----|--------|----------|
| `CONTROL_BEHAVIOR_DEFAULT` | 0 | `DefaultController` | **固定阈值（计数器）**，直接拒绝 |
| `CONTROL_BEHAVIOR_WARM_UP` | 1 | `WarmUpController` | **令牌桶 + 冷启动预热** |
| `CONTROL_BEHAVIOR_RATE_LIMITER` | 2 | `ThrottlingController` | **匀速排队（漏桶）** |
| `CONTROL_BEHAVIOR_WARM_UP_RATE_LIMITER` | 3 | `WarmUpRateLimiterController` | **预热 + 匀速排队** |

> 此外，1.8.10 新增了一个独立的 `slots/block/flow/tokenbucket/` 令牌桶工具包（`DefaultTokenBucket` / `StrictTokenBucket`），但目前**尚未接入 `generateRater` 路由**，属于预留的独立实现，文末单独说明。
>
> 除限流外，Sentinel 的**熔断降级**和**系统自适应限流**也各自实现了独立的"算法"，本文一并覆盖。

所有算法的统计数据来源都是**滑动窗口**（`LeapArray`，详见《Sentinel滑动窗口实现原理深度分析》），通过 `Node` 接口暴露的 `passQps()`、`previousPassQps()`、`curThreadNum()`、`avgRt()` 等方法读取实时指标。

---

## 一、DefaultController —— 固定阈值 / 计数器算法（快速失败）

> 源码：`slots/block/flow/controller/DefaultController.java`

### 1.1 算法本质

最朴素的**固定阈值计数器**：统计当前窗口内已使用的"令牌"（QPS 或并发线程数），如果 `已用 + 本次申请 > 阈值`，直接拒绝；否则放行。**超限即拒绝，不排队、不等待**，所以又叫"快速失败（fast-reject）"策略。

### 1.2 核心源码

```java
public boolean canPass(Node node, int acquireCount, boolean prioritized) {
    int curCount = avgUsedTokens(node);
    if (curCount + acquireCount > count) {
        // 超阈值：若是"高优先级 + QPS 维度"，尝试占用下一个窗口
        if (prioritized && grade == RuleConstant.FLOW_GRADE_QPS) {
            long currentTime = TimeUtil.currentTimeMillis();
            long waitInMs = node.tryOccupyNext(currentTime, acquireCount, count);
            if (waitInMs < OccupyTimeoutProperty.getOccupyTimeout()) {
                node.addWaitingRequest(currentTime + waitInMs, acquireCount);
                node.addOccupiedPass(acquireCount);
                sleep(waitInMs);
                throw new PriorityWaitException(waitInMs);  // 等待后放行
            }
        }
        return false;   // 直接拒绝
    }
    return true;
}

private int avgUsedTokens(Node node) {
    if (node == null) return 0;
    return grade == RuleConstant.FLOW_GRADE_THREAD
            ? node.curThreadNum()        // 线程数维度
            : (int)(node.passQps());     // QPS 维度
}
```

### 1.3 关键细节

- **两种统计维度**：`grade` 可为 `FLOW_GRADE_THREAD`（按并发线程数限流）或 `FLOW_GRADE_QPS`（按 QPS 限流）。线程数限流天然具备"阻塞即占用名额"的语义，无需等待。
- **优先级抢占（prioritized）**：当请求标记为高优先级且为 QPS 维度时，允许"借用"下一个窗口的额度（`tryOccupyNext`），让请求睡眠 `waitInMs` 后放行，并抛 `PriorityWaitException`（调用方据此识别是"排队通过"而非"直接通过"）。这是滑动窗口 `OccupiableBucketLeapArray` 的"未来桶占用"能力的使用点。若等待时间超过 `OccupyTimeout`，仍拒绝。
- **临界突刺问题**：纯计数器在窗口边界存在"双倍突刺"问题（例如阈值 100/s，在 0.9s~1.0s 通过 100 个、1.0s~1.1s 又通过 100 个，0.2s 内放行 200 个）。这是计数器算法的固有缺陷，Sentinel 通过滑动窗口的多桶设计一定程度平滑了它，但 `DefaultController` 本身仍是"窗口级计数"。

### 1.4 适用场景

绝大多数普通限流场景。简单、性能最好、对突发流量不做整形。

---

## 二、WarmUpController —— 预热 / 冷启动（基于令牌桶思想）

> 源码：`slots/block/flow/controller/WarmUpController.java`

### 2.1 算法本质

借鉴 **Guava 的 `RateLimiter` 冷启动算法**，但侧重点不同（源码注释明确说明）：
- Guava 关注**请求间隔调整**，类似漏桶；
- Sentinel 关注**每秒请求数控制**，类似令牌桶。

核心思想：用一个"令牌桶"的**剩余令牌数**衡量系统的"空闲/饱和"程度。系统冷启动时（长时间空闲），令牌桶是满的，允许的 QPS 较低；随着请求持续到来、令牌被消耗，系统逐渐"热"起来，允许的 QPS 逐步提升到稳定阈值 `count`。这避免了脉冲流量打垮一个"看似能扛、实则还在初始化"的系统（如 DB 建连、远程服务预热）。

### 2.2 关键参数推导（`construct` 方法）

```java
warningToken = (int)(warmUpPeriodInSec * count) / (coldFactor - 1);
maxToken = warningToken + (int)(2 * warmUpPeriodInSec * count / (1.0 + coldFactor));
slope = (coldFactor - 1.0) / count / (maxToken - warningToken);
```

- `count`：稳定期允许的 QPS（阈值）。
- `warmUpPeriodInSec`：预热时长（秒），默认 `FlowRule.warmUpPeriodSec = 10`。
- `coldFactor`：冷因子，默认 `ColdFactorProperty.coldFactor = 3`（必须 >1）。
- `warningToken`：警戒令牌线。剩余令牌高于此线 → 系统处于"冷/预热"区，QPS 受限。
- `maxToken`：令牌桶最大容量。
- `slope`：从冷速率到稳定速率的斜率（QPS 随令牌数变化的线性方程 `y = slope*x + b` 的斜率）。

### 2.3 限流判定（`canPass`）

```java
public boolean canPass(Node node, int acquireCount, boolean prioritized) {
    long passQps = (long) node.passQps();            // 当前窗口已通过 QPS
    long previousQps = (long) node.previousPassQps();// 上一窗口通过 QPS
    syncToken(previousQps);                          // 用上一窗口 QPS 同步令牌

    long restToken = storedTokens.get();
    if (restToken >= warningToken) {
        // 预热区：剩余令牌高于警戒线 → 允许的 QPS 较低（warningQps）
        long aboveToken = restToken - warningToken;
        double warningQps = Math.nextUp(1.0 / (aboveToken * slope + 1.0 / count));
        if (passQps + acquireCount <= warningQps) return true;
    } else {
        // 稳定区：剩余令牌低于警戒线 → 允许到稳定阈值 count
        if (passQps + acquireCount <= count) return true;
    }
    return false;
}
```

- `warningQps = 1 / (aboveToken * slope + 1/count)`：剩余令牌越多（越冷），分母越大，`warningQps` 越小，允许的 QPS 越低。随着令牌被消耗，`aboveToken` 减小，`warningQps` 增大，逐步逼近 `count`。这是一个**线性梯形**的 QPS 曲线。
- 一旦令牌降到 `warningToken` 以下，进入稳定区，直接按 `count` 限流。

### 2.4 令牌同步与冷却（`syncToken` / `coolDownTokens`）

```java
protected void syncToken(long passQps) {
    long currentTime = TimeUtil.currentTimeMillis();
    currentTime = currentTime - currentTime % 1000;   // 按秒对齐
    if (currentTime <= lastFilledTime.get()) return;   // 同秒内不重复填充

    long oldValue = storedTokens.get();
    long newValue = coolDownTokens(currentTime, passQps);
    if (storedTokens.compareAndSet(oldValue, newValue)) {
        // 扣除上一秒消耗的令牌
        long currentValue = storedTokens.addAndGet(0 - passQps);
        if (currentValue < 0) storedTokens.set(0L);
        lastFilledTime.set(currentTime);
    }
}
```

- **按秒填充**：每秒填充一次令牌（`currentTime % 1000` 对齐到秒边界）。
- **冷却（令牌恢复）逻辑** `coolDownTokens`：
  - 令牌低于 `warningToken`（系统较热）→ 持续按 `count` 速率补令牌；
  - 令牌高于 `warningToken`（系统较冷）→ 仅当上一窗口 `passQps < count/coldFactor`（请求很少）时才补令牌，否则不补，让系统"持续消耗"以加快预热。
  - 补充上限为 `maxToken`。
- **CAS 保证并发**：`storedTokens` 是 `AtomicLong`，用 `compareAndSet` 保证只有一个线程执行填充与扣减。

### 2.5 适用场景

需要预热的场景：应用启动初期、长时间空闲后突发流量、依赖需要初始化（DB 连接、远程服务、JIT 编译）的服务。避免冷系统被脉冲流量击垮。

---

## 三、ThrottlingController —— 匀速排队 / 漏桶算法

> 源码：`slots/block/flow/controller/ThrottlingController.java`（2.0 起重构自 1.x 的 `RateLimitController`）

### 3.1 算法本质

**匀速限流（leaky bucket / rate limiting）**：严格控制请求通过的时间间隔，使请求以恒定速率通过。超出的请求进入"虚拟队列"排队等待，超过最大排队时间则拒绝。所有通过的请求在时间轴上均匀分布，无突发。

### 3.2 核心源码

维护一个 `latestPassedTime`（上次请求通过的时间戳），每次请求计算"理论应通过时间"：

```java
private boolean checkPassUsingCachedMs(int acquireCount, double maxCountPerStat) {
    long currentTime = TimeUtil.currentTimeMillis();
    // 两次请求之间的间隔（costTime）
    long costTime = Math.round(1.0d * statDurationMs * acquireCount / maxCountPerStat);
    // 本请求的预期通过时间 = 上次通过时间 + 间隔
    long expectedTime = costTime + latestPassedTime.get();

    if (expectedTime <= currentTime) {
        // 预期时间已过 → 立即通过，更新基准时间
        latestPassedTime.set(currentTime);
        return true;
    } else {
        // 需要排队等待 waitTime
        long waitTime = costTime + latestPassedTime.get() - TimeUtil.currentTimeMillis();
        if (waitTime > maxQueueingTimeMs) return false;   // 超过最大排队时间 → 拒绝

        long oldTime = latestPassedTime.addAndGet(costTime);  // 占位
        waitTime = oldTime - TimeUtil.currentTimeMillis();
        if (waitTime > maxQueueingTimeMs) {
            latestPassedTime.addAndGet(-costTime);   // 回滚占位
            return false;
        }
        if (waitTime > 0) sleepMs(waitTime);          // 睡眠等待
        return true;
    }
}
```

### 3.3 关键细节

- **间隔公式**：`costTime = statDurationMs * acquireCount / maxCountPerStat`。例如阈值 200 QPS（`maxCountPerStat=200`, `statDurationMs=1000`），则单次请求间隔 `1000/200 = 5ms`，即每 5ms 放行一个请求。
- **`latestPassedTime` 是核心**：它是一个单调推进的"虚拟时间"。请求按到达顺序在其上累加 `costTime`，从而在时间轴上排成一列均匀分布的"通过时刻"。
- **`addAndGet` 占位 + 回滚**：用 CAS 类操作 `addAndGet(costTime)` 占位排队，若占位后发现等待超时，再 `addAndGet(-costTime)` 回滚。这是无锁排队的关键技巧。
- **纳秒精度切换**：当 `statDurationMs % count != 0`（间隔非整数毫秒）或 `count/statDurationMs > 1`（每毫秒多于 1 个请求）时，自动切到 `checkPassUsingNanoSeconds`，用 `System.nanoTime()` 和纳秒级间隔保证精度，并用 `LockSupport.parkNanos` 睡眠。
- **阻塞调用**：通过的请求会 `Thread.sleep` 真正阻塞等待，所以该算法会让调用线程"匀速化"。

### 3.4 适用场景

需要严格匀速的场景：消息队列消费、对下游有严格速率要求的 API 调用、削峰填谷。**不适合**普通 Web 入口（会阻塞线程，吞吐受限）。

---

## 四、WarmUpRateLimiterController —— 预热 + 匀速排队

> 源码：`slots/block/flow/controller/WarmUpRateLimiterController.java`

### 4.1 算法本质

`WarmUpController` 的子类，**结合预热与匀速排队**：先用预热逻辑算出当前允许的 QPS（冷启动时低、预热后高），再用 `ThrottlingController` 的"预期通过时间 + 排队等待"逻辑控制通过节奏。

### 4.2 核心源码

```java
public boolean canPass(Node node, int acquireCount, boolean prioritized) {
    long previousQps = (long) node.previousPassQps();
    syncToken(previousQps);                        // 复用预热的令牌同步

    long currentTime = TimeUtil.currentTimeMillis();
    long restToken = storedTokens.get();
    long costTime;
    if (restToken >= warningToken) {
        // 预热区：用 warningQps 计算间隔
        long aboveToken = restToken - warningToken;
        double warmingQps = Math.nextUp(1.0 / (aboveToken * slope + 1.0 / count));
        costTime = Math.round(1.0 * acquireCount / warmingQps * 1000);
    } else {
        // 稳定区：用 count 计算间隔
        costTime = Math.round(1.0 * acquireCount / count * 1000);
    }
    // 以下与 ThrottlingController 完全一致的排队等待逻辑
    long expectedTime = costTime + latestPassedTime.get();
    if (expectedTime <= currentTime) {
        latestPassedTime.set(currentTime);
        return true;
    } else {
        long waitTime = costTime + latestPassedTime.get() - currentTime;
        if (waitTime > timeoutInMs) return false;
        long oldTime = latestPassedTime.addAndGet(costTime);
        ... // 排队等待或回滚
    }
}
```

### 4.3 与前两者的关系

| 算法 | 预热 | 匀速排队 |
|------|:----:|:--------:|
| `DefaultController` | ✗ | ✗ |
| `WarmUpController` | ✓ | ✗ |
| `ThrottlingController` | ✗ | ✓ |
| `WarmUpRateLimiterController` | ✓ | ✓ |

它是预热与匀速的组合：冷启动期不仅 QPS 阈值低，通过的请求还匀速排队；预热完成后阈值升到 `count`，但仍匀速。

### 4.4 适用场景

既要预热保护、又要匀速削峰的场景。

---

## 五、独立的 TokenBucket 工具包（1.8.10 新增，预留）

> 源码：`slots/block/flow/tokenbucket/`

1.8.10 新增了一套独立的、**经典令牌桶**实现，但目前**未接入 `FlowRule` 路由**（`generateRater` 不引用它），属于独立工具/预留能力。

### 5.1 接口与抽象基类

```java
// TokenBucket.java
public interface TokenBucket {
    boolean tryConsume(long tokenNum);
    void refreshCurrentTokenNum(long timestamp);
}

// AbstractTokenBucket.java —— 经典令牌桶
protected volatile long currentTokenNum;    // 当前令牌数
protected volatile long nextProduceTime;    // 下次产令牌时间
protected final long unitProduceNum;        // 每单位时间产令牌数
protected final long maxTokenNum;           // 桶最大容量
```

`tryConsume` 逻辑：先 `refreshCurrentTokenNum`（按时间差补充令牌，上限 `maxTokenNum`），再判断令牌是否足够，够则扣减放行，不够则拒绝。

```java
public boolean tryConsume(long tokenNum) {
    if (tokenNum > maxTokenNum) return false;
    long currentTimestamp = TimeUtil.currentTimeMillis();
    refreshCurrentTokenNum(currentTimestamp);
    if (tokenNum <= currentTokenNum) {
        currentTokenNum -= tokenNum;
        return true;
    }
    return false;
}
```

令牌补充按"时间区间内包含几个生产周期"计算：
```java
protected long calProducedTokenNum(long currentTimestamp) {
    long nextRefreshUnitCount = (nextProduceTime - startTime) / intervalInMs;
    long currentUnitCount = (currentTimestamp - startTime) / intervalInMs;
    long unitCount = currentUnitCount - nextRefreshUnitCount + 1;
    return unitCount * unitProduceNum;
}
```

### 5.2 两种实现

- **`DefaultTokenBucket`**：非严格并发版，`currentTokenNum` 为 `volatile`，`tryConsume`/`refresh` 无锁，性能高但多线程下令牌扣减可能不精确（"Contestion may exist here, but it's okay"风格）。
- **`StrictTokenBucket`**：严格并发版，用 `refreshLock`（刷新锁）和 `consumeLock`（消费锁）+ double-check 保证令牌补充与扣减的准确性：

```java
public boolean tryConsume(long tokenNum) {
    ...
    refreshCurrentTokenNum(currentTimestamp);
    if (tokenNum <= currentTokenNum) {
        synchronized (consumeLock) {              // 消费锁
            if (tokenNum <= currentTokenNum) {     // double-check
                currentTokenNum -= tokenNum;
                return true;
            }
        }
    }
    return false;
}
public void refreshCurrentTokenNum(long currentTimestamp) {
    if (nextProduceTime > currentTimestamp) return;
    long producedTokenNum = calProducedTokenNum(currentTimestamp);
    synchronized (refreshLock) {                  // 刷新锁
        if (nextProduceTime > currentTimestamp) return;  // double-check
        currentTokenNum = Math.min(maxTokenNum, currentTokenNum + producedTokenNum);
        updateNextProduceTime(currentTimestamp);
    }
}
```

### 5.3 与 `WarmUpController` 的区别

`WarmUpController` 虽然也用"令牌"概念，但它是**冷启动预热**令牌桶（令牌数衡量系统冷热，QPS 随之变化）；而此处的 `TokenBucket` 是**经典固定速率**令牌桶（匀速产令牌、桶满则弃、请求消费令牌）。两者目标不同。

---

## 六、熔断降级算法（Circuit Breaker）

> 源码：`slots/block/degrade/circuitbreaker/`

熔断降级虽不属于 `FlowRule`，但同样是 Sentinel 的核心"控制算法"。它基于**状态机（CLOSED → OPEN → HALF_OPEN → CLOSED）**，统计则复用滑动窗口。

### 6.1 状态机（`AbstractCircuitBreaker`）

```java
protected final AtomicReference<State> currentState = new AtomicReference<>(State.CLOSED);
protected volatile long nextRetryTimestamp;   // 下次试探时间

public boolean tryPass(Context context) {
    if (currentState.get() == State.CLOSED)        return true;            // 闭合：放行
    if (currentState.get() == State.OPEN) {                                 // 开启：拒绝
        return retryTimeoutArrived() && fromOpenToHalfOpen(context);        // 到时间则转半开
    }
    return false;                                                           // HALF_OPEN：拒绝（只放探测请求）
}
```

- **CLOSED（闭合）**：正常放行，统计指标。
- **OPEN（开启）**：熔断，拒绝所有请求；到 `nextRetryTimestamp` 后转入 HALF_OPEN。
- **HALF_OPEN（半开）**：放行**一个探测请求**，根据其结果决定恢复（→CLOSED）或重新熔断（→OPEN）。

状态转换全部用 `AtomicReference.compareAndSet` 保证原子性，并通知观察者（`notifyObservers`）。

### 6.2 慢调用比例熔断（`ResponseTimeCircuitBreaker`）

> 算法：**慢调用比例（slow request ratio）**

```java
public void onRequestComplete(Context context) {
    SlowRequestCounter counter = slidingCounter.currentWindow().value();
    long rt = completeTime - entry.getCreateTimestamp();
    if (rt > maxAllowedRt) counter.slowCount.add(1);   // 慢调用计数
    counter.totalCount.add(1);                          // 总调用计数
    handleStateChangeWhenThresholdExceeded(rt);
}
// 判定：慢调用比例 > maxSlowRequestRatio 且总请求数 >= minRequestAmount → 熔断
double currentRatio = slowCount * 1.0d / totalCount;
if (currentRatio > maxSlowRequestRatio) transformToOpen(currentRatio);
```

- 统计结构 `SlowRequestCounter`：`slowCount`（慢调用数）+ `totalCount`（总调用数），用 `LongAdder`。
- 触发条件：在统计窗口内，总请求数达 `minRequestAmount` 后，慢调用比例超过 `slowRatioThreshold` 即熔断。
- `MAX_RATIO` 特殊处理：当阈值恰好为 1.0 且比例等于 1.0 时也触发（处理 `>` 不含等号的边界）。

### 6.3 异常熔断（`ExceptionCircuitBreaker`）

> 算法：**异常比例（exception ratio）/ 异常数（exception count）**

```java
public void onRequestComplete(Context context) {
    SimpleErrorCounter counter = stat.currentWindow().value();
    if (error != null) counter.getErrorCount().add(1);
    counter.getTotalCount().add(1);
    handleStateChangeWhenThresholdExceeded(error);
}
// 异常比例策略
if (strategy == DEGRADE_GRADE_EXCEPTION_RATIO) {
    curCount = errCount * 1.0d / totalCount;     // 异常比例
}
// 异常数策略：curCount = errCount
if (curCount > threshold) transformToOpen(curCount);
```

- 两种策略：`DEGRADE_GRADE_EXCEPTION_RATIO`（异常比例）与 `DEGRADE_GRADE_EXCEPTION_COUNT`（异常数）。
- 同样有 `minRequestAmount` 最小请求数保护，避免少量请求误判。

### 6.4 熔断器的滑动窗口

两个熔断器都内置了**自己的 `LeapArray` 子类**（`SlowRequestLeapArray`、`SimpleErrorCounterLeapArray`），桶内放各自的计数器。注意构造时 `sampleCount=1`，即**单桶窗口**（整个 `statIntervalMs` 作为一个桶），与限流的默认 2 桶不同——熔断更关注一个统计周期内的聚合比例，不需要细粒度滑动。

---

## 七、系统自适应限流（SystemRule + BBR）

> 源码：`slots/system/SystemRuleManager.java`

系统自适应限流从**全局系统指标**（而非单个资源）出发，结合多种维度判断，其中负载维度借鉴了 **BBR（Bottleneck Bandwidth and Round-trip propagation time）** 算法思想。

### 7.1 多维度检查（`checkSystem`）

```java
public static void checkSystem(ResourceWrapper resourceWrapper, int count) throws BlockException {
    // 仅对入口流量（EntryType.IN）生效
    // 1. 总 QPS
    if (currentQps + count > qps) throw ... "qps";
    // 2. 总线程数
    if (currentThread > maxThread) throw ... "thread";
    // 3. 平均 RT
    if (rt > maxRt) throw ... "rt";
    // 4. 系统负载（BBR）
    if (highestSystemLoadIsSet && getCurrentSystemAvgLoad() > highestSystemLoad) {
        if (!checkBbr(currentThread)) throw ... "load";
    }
    // 5. CPU 使用率
    if (highestCpuUsageIsSet && getCurrentCpuUsage() > highestCpuUsage) throw ... "cpu";
}
```

### 7.2 BBR 思想（`checkBbr`）

```java
private static boolean checkBbr(int currentThread) {
    if (currentThread > 1 &&
        currentThread > Constants.ENTRY_NODE.maxSuccessQps() * Constants.ENTRY_NODE.minRt() / 1000) {
        return false;   // 拒绝
    }
    return true;
}
```

BBR 的核心公式：**系统能承载的最大并发 ≈ 最大成功 QPS × 最小 RT / 1000**（即 `throughput × latency`，Little's Law 的体现）。当实际并发线程数超过这个"系统能力上界"时，认为系统过载，拒绝请求。它不是基于静态阈值，而是基于**系统实时吞吐能力**自适应判断——当系统变慢（RT 升高）或处理能力下降（maxSuccessQps 降低）时，允许的并发自动降低。

系统负载（`getCurrentSystemAvgLoad`）和 CPU 使用率（`getCurrentCpuUsage`）由 `SystemStatusListener` 后台线程采样（CPU 使用率仅 Linux 可用）。

### 7.3 适用场景

整机级别的保护，防止系统被拖垮。适合作为兜底保护，而非精细到资源的限流。

---

## 八、集群限流（Cluster Flow Control）

> 源码：`sentinel-cluster/sentinel-cluster-server-default/`

集群限流并非新算法，而是把**同一套阈值控制**从单机提升到集群维度：Token Server 维护全局配额，Token Client（各应用实例）向 Server 申请令牌。其底层统计仍用滑动窗口（`ClusterMetricLeapArray`、`ClusterParameterLeapArray`），限流判定本质与 `DefaultController` 一致（全局 QPS 是否超阈值）。当 Server 不可用时，可降级为本地限流。

---

## 九、算法对比总表

| 算法 | 控制器 / 类 | controlBehavior | 核心机制 | 超限行为 | 是否阻塞线程 | 突发流量 | 适用场景 |
|------|------------|:---:|----------|----------|:---:|:---:|----------|
| 固定阈值计数器 | `DefaultController` | 0 | 滑动窗口 QPS/线程数 vs 阈值 | 直接拒绝（优先级可借未来额度） | 否 | 允许（窗口内） | 通用限流 |
| 预热（冷启动） | `WarmUpController` | 1 | 令牌桶衡量冷热，QPS 线性提升 | 直接拒绝 | 否 | 预热期抑制 | 启动/空闲后突发 |
| 匀速排队（漏桶） | `ThrottlingController` | 2 | `latestPassedTime` + 间隔排队 | 排队超时拒绝 | 是 | 严格无突发 | 匀速削峰、MQ 消费 |
| 预热 + 匀速 | `WarmUpRateLimiterController` | 3 | 预热 QPS + 排队等待 | 排队超时拒绝 | 是 | 预热期抑制+匀速 | 预热 + 削峰 |
| 经典令牌桶（预留） | `DefaultTokenBucket`/`StrictTokenBucket` | 未接入 | 定速产令牌、消费令牌 | 拒绝 | 否 | 允许（受桶容量限制） | 独立工具，未路由 |
| 慢调用比例熔断 | `ResponseTimeCircuitBreaker` | —（DegradeRule） | 慢调用比例超阈值 → 状态机熔断 | 熔断 OPEN 拒绝 | 否 | — | 依赖慢/不稳定保护 |
| 异常熔断 | `ExceptionCircuitBreaker` | —（DegradeRule） | 异常比例/异常数超阈值 → 熔断 | 熔断 OPEN 拒绝 | 否 | — | 不稳定依赖保护 |
| 系统自适应（BBR） | `SystemRuleManager` | —（SystemRule） | 多维 + BBR 吞吐能力判断 | 拒绝 | 否 | — | 整机兜底保护 |

---

## 十、核心源码索引

| 文件 | 算法 |
|------|------|
| `slots/block/flow/TrafficShapingController.java` | 控制器统一接口 |
| `slots/block/flow/FlowRuleUtil.java`（`generateRater`） | 算法路由 |
| `slots/block/flow/controller/DefaultController.java` | 固定阈值计数器（快速失败） |
| `slots/block/flow/controller/WarmUpController.java` | 预热 / 冷启动 |
| `slots/block/flow/controller/ThrottlingController.java` | 匀速排队（漏桶） |
| `slots/block/flow/controller/WarmUpRateLimiterController.java` | 预热 + 匀速 |
| `slots/block/flow/tokenbucket/AbstractTokenBucket.java` | 经典令牌桶（预留） |
| `slots/block/flow/tokenbucket/DefaultTokenBucket.java` | 非严格并发令牌桶 |
| `slots/block/flow/tokenbucket/StrictTokenBucket.java` | 严格并发令牌桶 |
| `slots/block/degrade/circuitbreaker/AbstractCircuitBreaker.java` | 熔断状态机 |
| `slots/block/degrade/circuitbreaker/ResponseTimeCircuitBreaker.java` | 慢调用比例熔断 |
| `slots/block/degrade/circuitbreaker/ExceptionCircuitBreaker.java` | 异常比例/异常数熔断 |
| `slots/system/SystemRuleManager.java` | 系统自适应 + BBR |

---

## 十一、一句话总结

> Sentinel 的限流算法以 `TrafficShapingController` 为统一抽象，通过 `FlowRule.controlBehavior` 路由到四种实现：**`DefaultController`（计数器快速失败）**、**`WarmUpController`（令牌桶预热）**、**`ThrottlingController`（漏桶匀速排队）**、**`WarmUpRateLimiterController`（预热+匀速）**；1.8.10 还新增了**经典令牌桶**工具包（暂未路由）。所有算法的实时数据均来自 `LeapArray` 滑动窗口。此外，熔断降级用**状态机 + 慢调用比例/异常比例**实现，系统自适应限流借鉴 **BBR** 思想做整机保护——共同构成 Sentinel 的多维流量控制体系。