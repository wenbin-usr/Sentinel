# Sentinel 日志系统深度源码分析

> 基于 `D:\workspace\java_projects\source_projects\Sentinel` 源码（2.0.0-alpha2-SNAPSHOT 分支）
> 文档目的：完整梳理 Sentinel 的所有日志类型、落盘路径、写入机制、数据来源与文件存储策略

---

## 目录

- [一、整体概述](#一整体概述)
- [二、日志体系总览与文件清单](#二日志体系总览与文件清单)
- [三、日志 SPI 架构（Logger SPI 接口）](#三日志-spi-架构logger-spi-接口)
- [四、日志目录与配置加载（LogBase / LogConfigLoader）](#四日志目录与配置加载logbase--logconfigloader)
- [五、默认 JUL 实现深度解析](#五默认-jul-实现深度解析)
- [六、RecordLog —— 通用业务日志](#六recordlog--通用业务日志)
- [七、CommandCenterLog —— 命令中心日志](#七commandcenterlog--命令中心日志)
- [八、EagleEye 统计日志框架](#八eagleeye-统计日志框架)
- [九、EagleEyeLogUtil —— sentinel-block.log 限流日志](#九eagleeyeloutil--sentinel-blocklog-限流日志)
- [十、ClusterStatLogUtil 系列 —— 集群限流日志](#十clusterstatlogutil-系列--集群限流日志)
- [十一、MetricWriter —— metrics.log 指标日志](#十一metricwriter--metricslog-指标日志)
- [十二、LogSlot —— 限流拦截与日志记录](#十二logslot--限流拦截与日志记录)
- [十三、各模块日志输出全景](#十三各模块日志输出全景)
- [十四、日志落盘路径汇总](#十四日志落盘路径汇总)
- [十五、日志文件存储策略](#十五日志文件存储策略)
- [十六、配置项完整参考](#十六配置项完整参考)
- [十七、典型调用链路：一次限流请求的日志产生过程](#十七典型调用链路一次限流请求的日志产生过程)

---

## 一、整体概述

Sentinel 的日志系统不是单一的，而是**多套并行的日志体系**：

| 体系 | 用途 | 底层实现 | 典型文件 |
| --- | --- | --- | --- |
| **RecordLog** | 通用业务日志（生命周期、规则变更、心跳、初始化等） | JUL（默认）/ SLF4J / 自定义 SPI | `sentinel-record.log` |
| **CommandCenterLog** | 命令中心日志（HTTP 服务、请求处理、心跳发送） | JUL（默认）/ SLF4J / 自定义 SPI | `command-center.log` |
| **EagleEye** | 高性能统计日志（按时间窗口聚合，异步批量写） | 自研 `EagleEye` 框架 | `sentinel-block.log`、`sentinel-cluster.log` 等 |
| **MetricWriter** | 秒级指标日志（每个资源的通过/拒绝/RT） | 自研二进制 + 索引文件 | `${appName}-metrics.log.yyyy-MM-dd.N` |
| **EagleEye-self** | EagleEye 自身的运行日志 | EagleEye 自身 | `eagleeye-self.log` |

**核心理解**：
- RecordLog / CommandCenterLog 走 **Logger SPI** 机制（`com.alibaba.csp.sentinel.log.Logger` 接口），默认实现是 JUL，可通过 SPI 切换到 SLF4J/Logback 等。
- EagleEye 系列（block、cluster、metric 统计）走 **EagleEye 自研框架**，与 JUL 解耦，写入路径独立、性能优化（异步批写、按时间窗口聚合）。
- MetricWriter 是 Sentinel 的"监控数据持久化"，不走任何 Logger，直接 `FileOutputStream` 写二进制。

---

## 二、日志体系总览与文件清单

默认情况下（未做任何配置），Sentinel 启动后会在 `${user.home}/logs/csp/` 目录下生成以下文件：

| 文件名 | 产生类 | 写入方式 | 触发场景 | 默认大小上限 | 备份份数 | 切分策略 |
| --- | --- | --- | --- | --- | --- | --- |
| `sentinel-record.log` | `RecordLog` → `JavaLoggingAdapter` → `DateFileLogHandler` | 异步（线程池） | Sentinel 全生命周期业务事件 | 200MB | 4 份 | 按日期 + 大小切分 |
| `sentinel-record.log.yyyy-MM-dd.N` | 同上 | 同上 | 滚动备份 | 同上 | - | - |
| `command-center.log` | `CommandCenterLog` → `JavaLoggingAdapter` → `DateFileLogHandler` | 异步（线程池） | 命令中心 HTTP 服务 | 200MB | 4 份 | 按日期 + 大小切分 |
| `sentinel-block.log` | `EagleEyeLogUtil` → `EagleEye` 统计框架 | 异步（StatLogController） | 发生限流/熔断/系统保护等 BlockException | 300MB | 3 份 | 按大小切分 |
| `sentinel-cluster.log` | `ClusterStatLogUtil` | 异步 | 集群限流统计 | 300MB | 3 份 | 按大小切分 |
| `sentinel-cluster-client.log` | `ClusterClientStatLogUtil` | 异步 | 集群客户端统计 | 300MB | 3 份 | 按大小切分 |
| `sentinel-server.log` | `ClusterServerStatLogUtil` | 异步 | 集群服务端统计 | 300MB | 3 份 | 按大小切分 |
| `${appName}-metrics.log.yyyy-MM-dd.N` | `MetricWriter` | 同步（synchronized） | 每秒定时聚合 ClusterNode 指标 | 50MB（默认） | 6 份（默认） | 按日期 + 大小切分 |
| `${appName}-metrics.log.yyyy-MM-dd.N.idx` | `MetricWriter` | 同步 | 索引文件 | - | - | 与指标文件配对 |
| `eagleeye-self.log` | `EagleEye.selfLog` → `EagleEyeRollingFileAppender` | 同步（SyncAppender） | EagleEye 框架自身运行日志 | 200MB | 3 份 | 按大小切分 |

> 注意：上述文件路径默认都是 `${user.home}/logs/csp/`，因为 `LogBase.DIR_NAME = "logs" + File.separator + "csp"`，`EagleEye` 框架也使用 `LogBase.getLogBaseDir()` 作为基础路径。

---

## 三、日志 SPI 架构（Logger SPI 接口）

Sentinel 设计了一套日志 SPI 机制，允许用户在不修改源码的情况下替换日志实现。

### 3.1 SPI 接口：`Logger`

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/Logger.java`

```java
public interface Logger {
    void info(String format, Object... arguments);
    void info(String msg, Throwable e);
    void warn(String format, Object... arguments);
    void warn(String msg, Throwable e);
    void trace(String format, Object... arguments);
    void trace(String msg, Throwable e);
    void debug(String format, Object... arguments);
    void debug(String msg, Throwable e);
    void error(String format, Object... arguments);
    void error(String msg, Throwable e);
}
```

**关键设计**：占位符采用 SLF4J 风格 `{}`（注释明确说明 "the placeholder only supports the most popular placeholder convention (slf4j)"）。

### 3.2 SPI 加载器：`LoggerSpiProvider`

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/LoggerSpiProvider.java`

```java
public final class LoggerSpiProvider {
    private static final Map<String, Logger> LOGGER_MAP = new HashMap<>();
    static {
        try {
            resolveLoggers();
        } catch (Throwable t) {
            System.err.println("Failed to resolve Sentinel Logger SPI");
            t.printStackTrace();
        }
    }
    private static void resolveLoggers() {
        // NOTE: Here we cannot use SpiLoader directly because it depends on RecordLog.
        ServiceLoader<Logger> loggerLoader = ServiceLoader.load(Logger.class);
        for (Logger logger : loggerLoader) {
            LogTarget annotation = logger.getClass().getAnnotation(LogTarget.class);
            if (annotation == null) continue;
            String name = annotation.value();
            if (StringUtil.isNotBlank(name) && !LOGGER_MAP.containsKey(name)) {
                LOGGER_MAP.put(name, logger);
                System.out.println("Sentinel Logger SPI loaded for <" + name + ">: " + ...);
            }
        }
    }
}
```

**关键细节**：
- 使用 JDK 标准 `ServiceLoader<Logger>` 加载，**不走** `SpiLoader`（因为 `SpiLoader` 依赖 `RecordLog`，会循环依赖）
- 通过 `@LogTarget` 注解的 `value()` 区分不同 logger（如 `sentinelRecordLogger`、`sentinelCommandCenterLogger`）
- 同名 logger 只加载第一个（`!LOGGER_MAP.containsKey(name)`）

### 3.3 `@LogTarget` 注解

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/LogTarget.java`

```java
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
public @interface LogTarget {
    String value() default RecordLog.LOGGER_NAME;  // 默认 sentinelRecordLogger
}
```

### 3.4 官方提供的 SPI 实现：SLF4J 适配器

**文件位置**：`sentinel-logging/sentinel-logging-slf4j/src/main/java/com/alibaba/csp/sentinel/logging/slf4j/`

**SPI 注册文件**：`META-INF/services/com.alibaba.csp.sentinel.log.Logger`
```
com.alibaba.csp.sentinel.logging.slf4j.RecordLogLogger
com.alibaba.csp.sentinel.logging.slf4j.CommandCenterLogLogger
```

`RecordLogLogger` 示例（`@LogTarget(RecordLog.LOGGER_NAME)`）：
```java
@LogTarget(RecordLog.LOGGER_NAME)
public class RecordLogLogger implements Logger {
    private final org.slf4j.Logger logger = LoggerFactory.getLogger(RecordLog.LOGGER_NAME);
    // 全部委托给 slf4j Logger
}
```

`CommandCenterLogLogger` 示例（`@LogTarget(CommandCenterLog.LOGGER_NAME)`）：
```java
@LogTarget(CommandCenterLog.LOGGER_NAME)
public class CommandCenterLogLogger implements Logger {
    private final org.slf4j.Logger logger = LoggerFactory.getLogger(CommandCenterLog.LOGGER_NAME);
}
```

> 用户引入 `sentinel-logging-slf4j` 依赖后，RecordLog 与 CommandCenterLog 全部切换到 SLF4J，可通过 Logback/Log4j2 等做任何控制。

### 3.5 默认 SPI 回退：`JavaLoggingAdapter`

如果 `LoggerSpiProvider.getLogger(name)` 返回 `null`（即用户没引入 slf4j 适配器），则 fallback 到 `JavaLoggingAdapter`（基于 `java.util.logging`）。

```java
// RecordLog 静态块
static {
    logger = LoggerSpiProvider.getLogger(LOGGER_NAME);   // 先尝试 SPI
    if (logger == null) {
        logger = new JavaLoggingAdapter(LOGGER_NAME, DEFAULT_LOG_FILENAME);  // 回退到 JUL
    }
}
```

---

## 四、日志目录与配置加载（LogBase / LogConfigLoader）

### 4.1 `LogBase` —— 日志基础配置

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/LogBase.java`

#### 4.1.1 关键常量

```java
public static final String LOG_DIR = "csp.sentinel.log.dir";              // 日志目录配置 key
public static final String LOG_NAME_USE_PID = "csp.sentinel.log.use.pid"; // 文件名是否带 pid
public static final String LOG_OUTPUT_TYPE = "csp.sentinel.log.output.type"; // file 或 console
public static final String LOG_CHARSET = "csp.sentinel.log.charset";     // 字符集
public static final String LOG_LEVEL = "csp.sentinel.log.level";          // 日志级别

public static final String LOG_OUTPUT_TYPE_FILE = "file";
public static final String LOG_OUTPUT_TYPE_CONSOLE = "console";
public static final String LOG_CHARSET_UTF8 = "utf-8";

private static final String DIR_NAME = "logs" + File.separator + "csp";   // 默认子目录
private static final String USER_HOME = "user.home";
private static final Level LOG_DEFAULT_LEVEL = Level.INFO;                // 默认级别
```

#### 4.1.2 静态字段

```java
private static boolean logNameUsePid;       // 文件名是否带 pid
private static String logOutputType;         // 输出类型 file/console
private static String logBaseDir;            // 日志根目录（保证以 separator 结尾）
private static String logCharSet;            // 字符集
private static Level logLevel;              // 日志级别
```

#### 4.1.3 初始化流程（static 块）

```java
static {
    try {
        initializeDefault();    // 1. 设置默认值
        loadProperties();       // 2. 从配置文件覆盖
    } catch (Throwable t) {
        System.err.println("[LogBase] FATAL ERROR when initializing logging config");
        t.printStackTrace();
    }
}

private static void initializeDefault() {
    logNameUsePid = false;
    logOutputType = LOG_OUTPUT_TYPE_FILE;
    logBaseDir = addSeparator(System.getProperty(USER_HOME)) + DIR_NAME + File.separator;
    // 即 ${user.home}/logs/csp/
    logCharSet = LOG_CHARSET_UTF8;
    logLevel = LOG_DEFAULT_LEVEL;
}
```

#### 4.1.4 `loadProperties()` —— 配置覆盖

```java
private static void loadProperties() {
    Properties properties = LogConfigLoader.getProperties();

    // 输出类型（非法值回退 file）
    logOutputType = properties.get(LOG_OUTPUT_TYPE) == null ? logOutputType : properties.getProperty(LOG_OUTPUT_TYPE);
    if (!LOG_OUTPUT_TYPE_FILE.equalsIgnoreCase(logOutputType)
        && !LOG_OUTPUT_TYPE_CONSOLE.equalsIgnoreCase(logOutputType)) {
        logOutputType = LOG_OUTPUT_TYPE_FILE;
    }

    // 字符集
    logCharSet = properties.getProperty(LOG_CHARSET) == null ? logCharSet : properties.getProperty(LOG_CHARSET);

    // 日志目录：如果配置了就用配置的，否则保持默认 ${user.home}/logs/csp/
    logBaseDir = properties.getProperty(LOG_DIR) == null ? logBaseDir : properties.getProperty(LOG_DIR);
    logBaseDir = addSeparator(logBaseDir);   // 确保以分隔符结尾
    File dir = new File(logBaseDir);
    if (!dir.exists()) {
        if (!dir.mkdirs()) {                 // 不存在则 mkdirs
            System.err.println("ERROR: create Sentinel log base directory error: " + logBaseDir);
        }
    }

    // 文件名是否带 pid
    String usePid = properties.getProperty(LOG_NAME_USE_PID);
    logNameUsePid = "true".equalsIgnoreCase(usePid);

    // 日志级别
    String logLevelString = properties.getProperty(LOG_LEVEL);
    if (logLevelString != null && (logLevelString = logLevelString.trim()).length() > 0) {
        try {
            logLevel = Level.parse(logLevelString);
        } catch (IllegalArgumentException e) {
            // 回退默认 INFO
        }
    }
}
```

**注意**：`LogBase` 在初始化过程中通过 `System.out.println` 输出多行 INFO 日志，启动时控制台可见：
```
INFO: Sentinel log output type is: file
INFO: Sentinel log charset is: utf-8
INFO: Sentinel log base directory is: /root/logs/csp/
INFO: Sentinel log name use pid is: false
INFO: Sentinel log level is: INFO
```

### 4.2 `LogConfigLoader` —— 配置文件加载器

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/LogConfigLoader.java`

```java
public class LogConfigLoader {
    public static final String LOG_CONFIG_ENV_KEY = "CSP_SENTINEL_CONFIG_FILE";
    public static final String LOG_CONFIG_PROPERTY_KEY = "csp.sentinel.config.file";
    private static final String DEFAULT_LOG_CONFIG_FILE = "classpath:sentinel.properties";

    private static final Properties properties = new Properties();

    static {
        try {
            load();
        } catch (Throwable t) {
            // NOTE: do not use RecordLog here, or there will be circular class dependency!
            System.err.println("[LogConfigLoader] Failed to initialize configuration items");
            t.printStackTrace();
        }
    }

    private static void load() {
        // 优先级顺序：system property -> system env -> default file (classpath:sentinel.properties)
        String fileName = System.getProperty(LOG_CONFIG_PROPERTY_KEY);
        if (StringUtil.isBlank(fileName)) {
            fileName = System.getenv(LOG_CONFIG_ENV_KEY);
            if (StringUtil.isBlank(fileName)) {
                fileName = DEFAULT_LOG_CONFIG_FILE;
            }
        }
        Properties p = ConfigUtil.loadProperties(fileName);
        if (p != null && !p.isEmpty()) {
            properties.putAll(p);
        }
        // System Properties 覆盖配置文件（最高优先级）
        CopyOnWriteArraySet<Map.Entry<Object, Object>> copy = new CopyOnWriteArraySet<>(System.getProperties().entrySet());
        for (Map.Entry<Object, Object> entry : copy) {
            properties.put(entry.getKey().toString(), entry.getValue().toString());
        }
    }
}
```

**关键设计点**：
1. 配置加载顺序：`-Dcsp.sentinel.config.file` > 环境变量 `CSP_SENTINEL_CONFIG_FILE` > `classpath:sentinel.properties`
2. System Properties（`-D` 参数）的优先级最高，会覆盖配置文件中的同名项
3. 注释明确指出：**不能在此处使用 RecordLog，否则会循环依赖**（LogConfigLoader → RecordLog → LogBase → LogConfigLoader）

### 4.3 `ConfigUtil.loadProperties` —— 文件定位策略

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/util/ConfigUtil.java`

```java
public static Properties loadProperties(String fileName) {
    if (StringUtil.isNotBlank(fileName)) {
        if (absolutePathStart(fileName)) {
            return loadPropertiesFromAbsoluteFile(fileName);     // 绝对路径
        } else if (fileName.startsWith(CLASSPATH_FILE_FLAG)) { // "classpath:" 前缀
            return loadPropertiesFromClasspathFile(fileName);
        } else {
            return loadPropertiesFromRelativeFile(fileName);    // 相对 user.dir
        }
    }
    return null;
}
```

**字符集处理**（避免循环依赖）：
```java
private static Charset getCharset() {
    // avoid static loop dependencies: SentinelConfig -> SentinelConfigLoader -> ConfigUtil -> SentinelConfig
    return Charset.forName(System.getProperty("csp.sentinel.charset", StandardCharsets.UTF_8.name()));
}
```

### 4.4 `PidUtil` —— 进程 ID 获取

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/util/PidUtil.java`

```java
public static int getPid() {
    // Note: this will trigger local host resolve, which might be slow.
    String name = ManagementFactory.getRuntimeMXBean().getName();  // 形如 12345@hostname
    return Integer.parseInt(name.split("@")[0]);
}
```

当 `csp.sentinel.log.use.pid=true` 时，文件名会拼接 `.pid${pid}`，用于同一台机器多实例场景。

---

## 五、默认 JUL 实现深度解析

当未引入 slf4j 适配器时，`RecordLog` 与 `CommandCenterLog` 都使用 `JavaLoggingAdapter`，这是 Sentinel 默认的日志实现，下面深入剖析。

### 5.1 `JavaLoggingAdapter` —— Logger SPI 的默认实现

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/jul/JavaLoggingAdapter.java`

```java
public class JavaLoggingAdapter extends BaseJulLogger implements Logger {
    private final String loggerName;
    private final String fileNamePattern;          // 文件名 pattern（不含日期）
    private final java.util.logging.Logger julLogger;
    private final Handler logHandler;

    public JavaLoggingAdapter(String loggerName, String fileNamePattern) {
        AssertUtil.assertNotBlank(loggerName, "loggerName cannot be blank");
        AssertUtil.assertNotBlank(fileNamePattern, "fileNamePattern cannot be blank");
        this.loggerName = loggerName;
        this.fileNamePattern = fileNamePattern;
        this.julLogger = java.util.logging.Logger.getLogger(loggerName);  // 从 JUL 全局获取
        this.logHandler = makeLoggingHandler(fileNamePattern, julLogger); // 构造 handler
    }

    @Override
    public void info(String format, Object... arguments) {
        log(julLogger, logHandler, Level.INFO, format, arguments);
    }
    // ... warn/trace/debug/error 类似，级别不同
}
```

**注意**：`JavaLoggingAdapter` 自身**不持有** Logger 状态，所有操作委托给 `java.util.logging.Logger` 和 `Handler`。

### 5.2 `BaseJulLogger` —— JUL 日志工具基类

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/jul/BaseJulLogger.java`

#### 5.2.1 日志写入方法（带占位符替换）

```java
protected void log(Logger logger, Handler handler, Level level, String detail, Object... params) {
    if (detail == null) return;
    disableOtherHandlers(logger, handler);
    // 兼容 SLF4J 风格 {} 占位符
    FormattingTuple formattingTuple = MessageFormatter.arrayFormat(detail, params);
    String message = formattingTuple.getMessage();
    logger.log(level, message);
}
```

#### 5.2.2 `makeLoggingHandler` —— 构造日志处理器（核心）

```java
protected Handler makeLoggingHandler(String logName, Logger heliumRecordLog) {
    CspFormatter formatter = new CspFormatter();
    String logCharSet = LogBase.getLogCharset();
    Handler handler = null;
    switch (LogBase.getLogOutputType()) {
        case LOG_OUTPUT_TYPE_FILE:
            String fileName = LogBase.getLogBaseDir() + logName;       // 例如 /root/logs/csp/sentinel-record
            if (LogBase.isLogNameUsePid()) {
                fileName += ".pid" + PidUtil.getPid();                 // 可选带 pid
            }
            try {
                // 关键：构造 DateFileLogHandler
                // pattern: sentinel-record.%d   （%d 在 rotateDate 时被替换为 yyyy-MM-dd）
                // limit: 1024 * 1024 * 200 = 200MB
                // count: 4（保留 4 个备份）
                // append: true
                handler = new DateFileLogHandler(fileName + ".%d", 1024 * 1024 * 200, 4, true);
                handler.setFormatter(formatter);
                handler.setEncoding(logCharSet);
                handler.setLevel(LogBase.getLogLevel());
            } catch (IOException e) {
                e.printStackTrace();
            }
            break;
        case LOG_OUTPUT_TYPE_CONSOLE:
            try {
                handler = new ConsoleHandler();
                handler.setFormatter(formatter);
                handler.setEncoding(logCharSet);
                handler.setLevel(LogBase.getLogLevel());
            } catch (IOException e) {
                e.printStackTrace();
            }
            break;
        default:
            break;
    }
    if (handler != null) {
        disableOtherHandlers(heliumRecordLog, handler);  // 移除其他 handler，避免重复
    }
    heliumRecordLog.setLevel(LogBase.getLogLevel());
    return handler;
}
```

**核心参数（硬编码）**：
- 单文件大小上限：`200MB`（`1024 * 1024 * 200`）
- 备份文件数：`4`
- 文件名 pattern：`{logBaseDir}{logName}.%d`，例如 `/root/logs/csp/sentinel-record.%d`
- 是否追加：`true`

#### 5.2.3 `disableOtherHandlers` —— 防重复 handler

```java
static void disableOtherHandlers(Logger logger, Handler handler) {
    if (logger == null) return;
    synchronized (logger) {
        Handler[] handlers = logger.getHandlers();
        if (handlers == null) return;
        if (handlers.length == 1 && handlers[0].equals(handler)) return; // 已是唯一 handler，跳过
        logger.setUseParentHandlers(false);  // 不向父 logger 传递（避免 JUL 全局 console 输出）
        for (Handler h : handlers) {
            logger.removeHandler(h);         // 移除所有现有 handler
        }
        logger.addHandler(handler);          // 只挂上自己的 handler
    }
}
```

> 此方法会在每次 `log()` 调用时检查并执行，确保 JUL 全局配置不会干扰 Sentinel 自身的 handler。`setUseParentHandlers(false)` 是关键，否则日志会被父 Logger（如 root logger）的 ConsoleHandler 重复输出。

### 5.3 `DateFileLogHandler` —— 按日期切分文件处理器（重点）

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/jul/DateFileLogHandler.java`

这是 Sentinel 默认 JUL 实现的核心，**同时支持按日期切分和按大小切分**。

#### 5.3.1 关键字段

```java
class DateFileLogHandler extends Handler {
    // 日期格式化（用于文件名 %d 替换）
    private final DateTimeFormatter dateFormat = DateTimeFormatter
        .ofPattern("yyyy-MM-dd")
        .withZone(ZoneId.systemDefault());

    // 异步写日志的线程池
    private static final ThreadPoolExecutor executor = new ThreadPoolExecutor(
        1,                                       // corePoolSize = 1
        5,                                       // maxPoolSize = 5
        1, TimeUnit.HOURS,                       // keepAlive 1 小时
        new ArrayBlockingQueue<Runnable>(1024),  // 队列 1024
        new NamedThreadFactory("sentinel-datafile-log-executor", true),  // daemon 线程
        new ThreadPoolExecutor.DiscardOldestPolicy()  // 拒绝策略：丢弃最老的
    );
    static { executor.allowCoreThreadTimeOut(true); }

    private volatile FileHandler handler;          // 真正写文件的 JUL FileHandler
    private final String pattern;                   // 文件名 pattern（含 %d）
    private final int limit;                        // 单文件大小上限
    private final int count;                        // 备份文件数
    private final boolean append;                   // 是否追加
    private volatile boolean initialized = false;
    private volatile long startDate = System.currentTimeMillis();
    private volatile long endDate;                  // 当前文件的生命周期终点（明天 0 点）
    private final Object monitor = new Object();    // rotateDate 同步锁
}
```

#### 5.3.2 构造器与初始 rotateDate

```java
DateFileLogHandler(String pattern, int limit, int count, boolean append) throws SecurityException {
    this.pattern = pattern;
    this.limit = limit;
    this.count = count;
    this.append = append;
    rotateDate();                  // 构造时立即初始化第一个文件
    this.initialized = true;
}
```

#### 5.3.3 `rotateDate()` —— 日期滚动核心

```java
private void rotateDate() {
    this.startDate = System.currentTimeMillis();
    if (handler != null) {
        handler.close();                            // 关闭旧 handler
    }
    // 用当前日期替换 pattern 中的 %d
    String newPattern = pattern.replace("%d", dateFormat.format(Instant.now()));
    // 计算明天 0 点的时间戳，作为 endDate
    Calendar next = Calendar.getInstance();
    next.set(Calendar.HOUR_OF_DAY, 0);
    next.set(Calendar.MINUTE, 0);
    next.set(Calendar.SECOND, 0);
    next.set(Calendar.MILLISECOND, 0);
    next.add(Calendar.DATE, 1);
    this.endDate = next.getTimeInMillis();
    try {
        // 用 JUL 的 FileHandler 完成实际写入 + 大小切分
        this.handler = new FileHandler(newPattern, limit, count, append);
        if (initialized) {
            // 重新挂载 handler 后，要复制 formatter/encoding/level 等配置
            handler.setEncoding(this.getEncoding());
            handler.setErrorManager(this.getErrorManager());
            handler.setFilter(this.getFilter());
            handler.setFormatter(this.getFormatter());
            handler.setLevel(this.getLevel());
        }
    } catch (SecurityException | IOException e) {
        e.printStackTrace();
    }
}
```

**关键理解**：
- `pattern` 中的 `%d` 被替换为当天日期，例如 `sentinel-record.%d` → `sentinel-record.2026-08-12`
- JUL `FileHandler` 还会按 `limit`（200MB）和 `count`（4）做大小切分，生成的文件名形如 `sentinel-record.2026-08-12.0`、`sentinel-record.2026-08-12.1`、...、`sentinel-record.2026-08-12.3`
- 每天到 0 点会触发一次 `rotateDate`，切到新日期文件

#### 5.3.4 `publish()` —— 异步写入

```java
@Override
public void publish(LogRecord record) {
    if (shouldRotate(record)) {                         // 检查是否需要切分
        synchronized (monitor) {
            if (shouldRotate(record)) {                 // 双重检查
                rotateDate();
            }
        }
    }
    // 检测"漏切分"（系统挂起超过 25 小时）
    if (System.currentTimeMillis() - startDate > 25 * 60 * 60 * 1000) {
        String msg = record.getMessage();
        record.setMessage("missed file rolling at: " + new Date(endDate) + "\n" + msg);
    }
    executor.execute(new LogTask(record, handler));     // 异步提交写任务
}

private boolean shouldRotate(LogRecord record) {
    if (endDate <= record.getMillis() || !logFileExits()) {
        return true;                                    // 已跨天 或 文件被外部删除
    }
    return false;
}
```

#### 5.3.5 内部类 `LogTask` 与 `LogRejectedExecutionHandler`

```java
static class LogTask implements Runnable {
    private final LogRecord record;
    private final FileHandler handler;
    public LogTask(LogRecord record, FileHandler handler) {
        this.record = record;
        this.handler = handler;
    }
    public void run() {
        handler.publish(record);                        // 委托给 JUL FileHandler
    }
}

static class LogRejectedExecutionHandler implements RejectedExecutionHandler {
    private final long recordPeriod;
    private Long lastRecordTime;
    public LogRejectedExecutionHandler() {
        recordPeriod = Long.parseLong(System.getProperty(
            "sentinel.rejected.record.period", "60000"));  // 默认 60 秒打印一次拒绝
    }
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        long currentTimestamp = System.currentTimeMillis();
        if (lastRecordTime == null || currentTimestamp - lastRecordTime > recordPeriod) {
            System.err.println("Failed to log sentinel record with datafile, rejected");
            lastRecordTime = currentTimestamp;
        }
    }
}
```

**注意**：代码中静态 `executor` 字段在内部类 `LogRejectedExecutionHandler` 中并未真正使用（构造 ConsoleHandler 时才用到），这里是 DateFileLogHandler 用的拒绝策略是构造 executor 时直接传入的 `DiscardOldestPolicy`。

### 5.4 `ConsoleHandler` —— 控制台输出处理器

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/jul/ConsoleHandler.java`

当 `csp.sentinel.log.output.type=console` 时使用。

#### 5.4.1 关键设计

```java
class ConsoleHandler extends Handler {
    private static final ThreadPoolExecutor executor = new ThreadPoolExecutor(
        1, 5, 1, TimeUnit.HOURS,
        new ArrayBlockingQueue<>(1024),
        new NamedThreadFactory("sentinel-console-log-executor", true),
        new LogRejectedExecutionHandler()
    );
    static { executor.allowCoreThreadTimeOut(true); }

    private StreamHandler stdoutHandler;  // INFO 及以下 → System.out
    private StreamHandler stderrHandler;  // WARNING 及以上 → System.err
    private AtomicReference<Future<?>> lastFuture = new AtomicReference<>();

    public ConsoleHandler() {
        this.stdoutHandler = new StreamHandler(System.out, new CspFormatter());
        this.stderrHandler = new StreamHandler(System.err, new CspFormatter());
    }

    @Override
    public void publish(LogRecord record) {
        lastFuture.set(executor.submit(new LogTask(record, stdoutHandler, stderrHandler)));
    }

    static class LogTask implements Runnable {
        public void run() {
            if (record.getLevel().intValue() >= Level.WARNING.intValue()) {
                stderrHandler.publish(record);  // 警告以上 → stderr
                stderrHandler.flush();
            } else {
                stdoutHandler.publish(record);  // 其他 → stdout
                stdoutHandler.flush();
            }
        }
    }
}
```

**关键特性**：
- 同样使用线程池异步写
- 按 level 区分 stdout / stderr
- `close()` 会等待最后一个 Future 完成（`future.get()`），保证关闭时不丢日志

### 5.5 `CspFormatter` —— 日志格式

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/jul/CspFormatter.java`

```java
class CspFormatter extends Formatter {
    private final DateTimeFormatter dateFormat = DateTimeFormatter
        .ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
        .withZone(ZoneId.systemDefault());

    @Override
    public String format(LogRecord record) {
        StringBuilder builder = new StringBuilder(1000);
        builder.append(dateFormat.format(Instant.ofEpochMilli(record.getMillis()))).append(" ");
        builder.append(record.getLevel().getName()).append(" ");
        builder.append(formatMessage(record));
        String throwable = "";
        if (record.getThrown() != null) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.println();
            record.getThrown().printStackTrace(pw);
            pw.close();
            throwable = sw.toString();
        }
        builder.append(throwable);
        if ("".equals(throwable)) {
            builder.append("\n");   // 没异常时补换行
        }
        return builder.toString();
    }
}
```

**最终日志格式**：
```
2026-08-12 14:30:25.123 INFO [Sentinel Starter] Sentinel log output type is: file
2026-08-12 14:30:26.456 WARN Unexpected entry exception
java.lang.RuntimeException: ...
    at ...
```

格式为：`yyyy-MM-dd HH:mm:ss.SSS LEVEL 消息内容 [换行后是异常堆栈]`

### 5.6 `Level` —— 自定义日志级别

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/jul/Level.java`

```java
public class Level extends java.util.logging.Level {
    public static final Level ERROR = new Level("ERROR", 1000);
    public static final Level WARNING = new Level("WARNING", 900);
    public static final Level INFO = new Level("INFO", 800);
    public static final Level DEBUG = new Level("DEBUG", 700);
    public static final Level TRACE = new Level("TRACE", 600);
}
```

**注意**：相比 JUL 标准 7 级（SEVERE/WARNING/INFO/CONFIG/FINE/FINER/FINEST），Sentinel 简化为 5 级，且 `Level.parse(String)` 在 `LogBase` 中支持字符串配置。

### 5.7 `MessageFormatter` 与 `FormattingTuple`

**文件**：
- `sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/jul/MessageFormatter.java`
- `sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/jul/FormattingTuple.java`

代码注释明确：**This code was copied from SLF4J which licensed under the MIT License.**

`MessageFormatter.arrayFormat(detail, params)` 实现了 SLF4J 风格的 `{}` 占位符替换，使 `RecordLog.info("msg: {}", value)` 能正常工作。

`FormattingTuple` 持有 `message`、`throwable`、`argArray` 三元信息。

---

## 六、RecordLog —— 通用业务日志

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/RecordLog.java`

### 6.1 类定义

```java
public class RecordLog {
    public static final String LOGGER_NAME = "sentinelRecordLogger";
    public static final String DEFAULT_LOG_FILENAME = "sentinel-record.log";

    private static com.alibaba.csp.sentinel.log.Logger logger = null;

    static {
        try {
            logger = LoggerSpiProvider.getLogger(LOGGER_NAME);    // 先尝试 SPI
            if (logger == null) {
                logger = new JavaLoggingAdapter(LOGGER_NAME, DEFAULT_LOG_FILENAME);  // fallback
            }
        } catch (Throwable t) {
            System.err.println("Error: failed to initialize Sentinel RecordLog");
            t.printStackTrace();
        }
    }

    public static void info(String format, Object... arguments) { logger.info(format, arguments); }
    public static void info(String msg, Throwable e)            { logger.info(msg, e); }
    public static void warn(String format, Object... arguments) { logger.warn(format, arguments); }
    public static void warn(String msg, Throwable e)            { logger.warn(msg, e); }
    public static void trace(String format, Object... arguments){ logger.trace(format, arguments); }
    public static void trace(String msg, Throwable e)           { logger.trace(msg, e); }
    public static void debug(String format, Object... arguments){ logger.debug(format, arguments); }
    public static void debug(String msg, Throwable e)           { logger.debug(msg, e); }
    public static void error(String format, Object... arguments){ logger.error(format, arguments); }
    public static void error(String msg, Throwable e)           { logger.error(msg, e); }

    private RecordLog() {}
}
```

### 6.2 默认文件名

`sentinel-record.log`，但实际落盘的文件名经过 `DateFileLogHandler` 处理后是 `sentinel-record.yyyy-MM-dd.N`（具体规则见 [5.3](#53-datefileloghandler--按日期切分文件处理器重点)）。

### 6.3 数据来源

`RecordLog` 是 Sentinel 中**使用最广泛**的日志，几乎所有模块都依赖它。下面列举主要调用点（参考 `RecordLog.info/warn/error` 在源码中的引用）：

| 模块 | 调用点 | 触发场景 |
| --- | --- | --- |
| `sentinel-core` | `Constants`、`SentinelConfig`、`CtSph`、`StatisticSlot`、`LogSlot`、`DefaultSlotChainBuilder` | Sentinel 启动、Slot Chain 构建、配置加载、Slot 异常 |
| `sentinel-transport-simple-http` | `SimpleHttpHeartbeatSender`、`SimpleHttpCommandCenter` | 心跳发送、控制台地址解析 |
| `sentinel-transport-netty-http` | `HttpHeartbeatSender`、`HttpServer` | 心跳、Netty 启动 |
| `sentinel-transport-common` | `HeartbeatSenderInitFunc`、`TransportConfig` | 心跳初始化、端口绑定 |
| `sentinel-cluster-client-default` | `DefaultClusterTokenClient`、`TokenClientHandler` | 集群客户端生命周期、连接事件 |
| `sentinel-cluster-server-default` | `NettyTransportServer`、`ConnectionManager`、`TokenServerHandler` | 集群服务端启动、连接管理 |
| `sentinel-extension` | `FileRefreshableDataSource`、`NacosDataSource`、`ApolloDataSource`、`ZookeeperDataSource` 等 | 数据源初始化、配置变更、加载异常 |
| `sentinel-adapter` | `SentinelDubboProviderFilter`、`SentinelDubboConsumerFilter` 等 | 适配器初始化 |
| `sentinel-cluster-flow-control` | `ClusterFlowRuleManager`、`ClusterRuleUtil` | 集群规则加载、规则校验 |

### 6.4 典型日志样例

```
2026-08-12 10:23:45.123 INFO [Sentinel Starter] Sentinel log output type is: file
2026-08-12 10:23:45.456 INFO [Sentinel Starter] Sentinel log base directory is: /root/logs/csp/
2026-08-12 10:23:46.789 INFO [HeartbeatSenderInitFunc] Current heart beat interval: 10000
2026-08-12 10:23:47.012 INFO [NacosDataSource] New property value received for (properties: {}) (dataId: sentinel-flow-rules, groupId: SENTINEL_GROUP): [{"app":"demo","resource":"hello","limitApp":"default","grade":1,"count":5}]
2026-08-12 10:24:01.345 WARN [FileRefreshableDataSource] File does not exist: /etc/sentinel/rules/flow.json
```

---

## 七、CommandCenterLog —— 命令中心日志

**文件**：`sentinel-transport/sentinel-transport-common/src/main/java/com/alibaba/csp/sentinel/transport/log/CommandCenterLog.java`

### 7.1 类定义（与 RecordLog 几乎完全一致）

```java
public class CommandCenterLog {
    public static final String LOGGER_NAME = "sentinelCommandCenterLogger";
    public static final String DEFAULT_LOG_FILENAME = "command-center.log";

    private static com.alibaba.csp.sentinel.log.Logger logger = null;

    static {
        try {
            logger = LoggerSpiProvider.getLogger(LOGGER_NAME);    // 先尝试 SPI
            if (logger == null) {
                logger = new JavaLoggingAdapter(LOGGER_NAME, DEFAULT_LOG_FILENAME);  // fallback
            }
        } catch (Throwable t) {
            System.err.println("Error: failed to initialize Sentinel CommandCenterLog");
            t.printStackTrace();
        }
    }
    // ... 同 RecordLog 一致的 info/warn/trace/debug/error 方法
}
```

### 7.2 数据来源

`CommandCenterLog` 专门记录 transport 命令中心相关的日志：

| 模块 | 调用点 | 触发场景 |
| --- | --- | --- |
| `sentinel-transport-simple-http` | `SimpleHttpCommandCenter`、`HttpEventTask` | HTTP 服务启动、Socket 请求处理、请求耗时 |
| `sentinel-transport-netty-http` | `NettyHttpCommandCenter`、`HttpServer`、`HttpServerHandler` | Netty 服务启动、bind 重试、请求处理异常 |
| `sentinel-transport-common` | `HttpEventTask`（基类） | 通用 HTTP 处理 |
| CommandHandler | 各种 `CommandHandler` 实现 | 命令处理日志（异常时） |

### 7.3 典型日志样例

```
2026-08-12 10:23:45.123 INFO [CommandCenter] Begin listening at port 8719
2026-08-12 10:24:01.234 INFO [SimpleHttpCommandCenter] Socket income: GET /getStat?..., addr: /127.0.0.1
2026-08-12 10:24:01.345 INFO [SimpleHttpCommandCenter] Deal a socket task: GET /getStat, address: /127.0.0.1, time cost: 12 ms
2026-08-12 10:24:30.567 WARN [HttpServerHandler] Internal error
java.lang.IllegalStateException: No compatible encoder
    at ...
```

### 7.4 与 RecordLog 的差异

| 维度 | RecordLog | CommandCenterLog |
| --- | --- | --- |
| logger name | `sentinelRecordLogger` | `sentinelCommandCenterLogger` |
| 默认文件名 | `sentinel-record.log` | `command-center.log` |
| 关注点 | Sentinel 业务生命周期 | 命令中心（HTTP 服务端、命令处理） |
| 调用模块 | 全模块 | 主要在 sentinel-transport-* |

两者底层共享同一套 JUL 机制、`DateFileLogHandler`、`CspFormatter`，只是文件名与 logger name 不同。

---

## 八、EagleEye 统计日志框架

EagleEye 是 Sentinel 自研的高性能统计日志框架（来自阿里巴巴 EagleEye 项目），用于记录**频繁发生的统计事件**（如限流、集群 token 请求）。与 RecordLog/CommandCenterLog 不同，它不走 Logger SPI，完全独立。

### 8.1 整体架构

```
                ┌──────────────────────────┐
                │   StatLogger (对外接口)   │
                │   stat(key).count()      │
                └────────────┬─────────────┘
                             │
                  ┌──────────▼──────────┐
                  │  StatRollingData    │  ← 当前时间窗口的累计数据（CAS 切换）
                  │  (Map<StatEntry,    │
                  │   StatEntryFunc>)   │
                  └──────────┬──────────┘
                             │ rolling() 按周期切换
                  ┌──────────▼──────────┐
                  │  StatLogController  │  ← 定时调度
                  │  (rollerThreadPool) │
                  │  (writerThreadPool) │
                  └──────────┬──────────┘
                             │
                  ┌──────────▼──────────┐
                  │ StatLogWriteTask    │
                  │ format: time|type|  │
                  │   keys|values\n     │
                  └──────────┬──────────┘
                             │
                  ┌──────────▼──────────┐
                  │ EagleEyeAppender    │
                  │   (SyncAppender →   │  ← 同步包装，串行化
                  │    RollingFileApp)  │
                  └──────────┬──────────┘
                             │
                  ┌──────────▼──────────┐
                  │ EagleEyeRollingFile │  ← 真正写文件 + 按大小切分
                  │     Appender        │
                  └─────────────────────┘
                  辅助：
                  - EagleEyeLogDaemon：每 20s 清理 .deleted 文件 + reload
```

### 8.2 `EagleEye` 入口类

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/eagleeye/EagleEye.java`

#### 8.2.1 路径解析

```java
public final class EagleEye {
    static final String USER_HOME = locateUserHome();
    static final String BASE_LOG_DIR = locateBaseLogPath();
    static final String EAGLEEYE_LOG_DIR = locateEagleEyeLogPath();
    static final String APP_LOG_DIR = locateAppLogPath();

    static final Charset DEFAULT_CHARSET = getDefaultOutputCharset();  // GB18030 > GBK > UTF-8

    static final String EAGLEEYE_SELF_LOG_FILE = EagleEye.EAGLEEYE_LOG_DIR + "eagleeye-self.log";
    static final long MAX_SELF_LOG_FILE_SIZE = 200 * 1024 * 1024;  // 200MB
    static EagleEyeAppender selfAppender = createSelfLogger();

    private static String locateUserHome() {
        String userHome = EagleEyeCoreUtils.getSystemProperty("user.home");
        if (EagleEyeCoreUtils.isNotBlank(userHome)) {
            if (!userHome.endsWith(File.separator)) userHome += File.separator;
        } else {
            userHome = "/tmp/";  // 极端 fallback
        }
        return userHome;
    }

    private static String locateBaseLogPath() {
        // JM.LOG.PATH 是 EagleEye 历史配置项
        String tmpPath = EagleEyeCoreUtils.getSystemProperty("JM.LOG.PATH");
        if (EagleEyeCoreUtils.isNotBlank(tmpPath)) {
            if (!tmpPath.endsWith(File.separator)) tmpPath += File.separator;
        } else {
            tmpPath = USER_HOME + "logs" + File.separator;
        }
        return tmpPath;
    }

    private static String locateEagleEyeLogPath() {
        String tmpPath = EagleEyeCoreUtils.getSystemProperty("EAGLEEYE.LOG.PATH");
        if (EagleEyeCoreUtils.isNotBlank(tmpPath)) {
            if (!tmpPath.endsWith(File.separator)) tmpPath += File.separator;
        } else {
            tmpPath = BASE_LOG_DIR + "eagleeye" + File.separator;
        }
        return tmpPath;
    }
}
```

**注意**：上面这些路径在 Sentinel 中实际**未使用**，因为 Sentinel 的所有 StatLoggerBuilder 都通过 `configLogFilePath(path)` 显式指定路径（路径来自 `LogBase.getLogBaseDir()` + 文件名）。EagleEye 自身的 `eagleeye-self.log` 才走 `EAGLEEYE_LOG_DIR`。

#### 8.2.2 字符集默认 GB18030

```java
static Charset getDefaultOutputCharset() {
    Charset cs;
    String charsetName = EagleEyeCoreUtils.getSystemProperty("EAGLEEYE.CHARSET");
    if (EagleEyeCoreUtils.isNotBlank(charsetName)) {
        cs = Charset.forName(charsetName.trim());
        if (cs != null) return cs;
    }
    try { cs = Charset.forName("GB18030"); }
    catch (Exception e) {
        try { cs = Charset.forName("GBK"); }
        catch (Exception e2) { cs = Charset.forName("UTF-8"); }
    }
    return cs;
}
```

**重要**：EagleEye 写入的 `sentinel-block.log`、`sentinel-cluster.log` 等默认字符集是 **GB18030**（中文环境优先），与 RecordLog 的 UTF-8 不同！这是历史遗留问题。

#### 8.2.3 自身日志：`selfLog`

```java
public static void selfLog(String log) {
    try {
        String timestamp = EagleEyeCoreUtils.formatTime(System.currentTimeMillis());
        String line = "[" + timestamp + "] " + log + EagleEyeCoreUtils.NEWLINE;
        selfAppender.append(line);
    } catch (Throwable t) { }
}

public static void selfLog(String log, Throwable e) {
    long now = System.currentTimeMillis();
    if (exceptionBucket.accept(now)) {   // 令牌桶限流（10s 内 10 个异常日志）
        // ... 写入异常堆栈
    }
}
```

#### 8.2.4 静态初始化（启动 EagleEye）

```java
static {
    initEagleEye();
}

private static void initEagleEye() {
    selfLog("[INFO] EagleEye started (" + CLASS_LOCATION + ")...");
    try { EagleEyeLogDaemon.start(); }    // 启动后台清理线程
    catch (Throwable e) { ... }
    try { StatLogController.start(); }    // 启动定时调度器
    catch (Throwable e) { ... }
}
```

### 8.3 `StatLogger` —— 对外接口

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/eagleeye/StatLogger.java`

```java
public final class StatLogger {
    private final String loggerName;
    private final EagleEyeAppender appender;
    private final AtomicReference<StatRollingData> ref;
    private final long intervalMillis;     // 切换周期（毫秒）
    private final int maxEntryCount;
    private final char entryDelimiter;
    private final char keyDelimiter;
    private final char valueDelimiter;

    StatLogger(...) {
        // ...
        this.ref = new AtomicReference<>();
        rolling();   // 初始化第一个 StatRollingData
    }

    // CAS 切换到新的 StatRollingData
    StatRollingData rolling() {
        do {
            long now = System.currentTimeMillis();
            long timeSlot = now - now % intervalMillis;  // 对齐到 interval 边界
            StatRollingData prevData = ref.get();
            long rollingTimeMillis = timeSlot + intervalMillis;
            int initialCapacity = prevData != null ? prevData.getStatCount() : 16;
            StatRollingData nextData = new StatRollingData(this, initialCapacity, timeSlot, rollingTimeMillis);
            if (ref.compareAndSet(prevData, nextData)) {
                return prevData;   // 返回旧数据，交给 StatLogController 写盘
            }
        } while (true);
    }

    // 多种 stat() 重载，返回 StatEntry，链式调用 .count() / .count(n)
    public StatEntry stat(String key) { return new StatEntry(this, key); }
    public StatEntry stat(String key1, String key2) { ... }
    // ... 最多 8 个 key，还有 String... 可变参数
}
```

### 8.4 `StatLoggerBuilder` —— 构造器

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/eagleeye/StatLoggerBuilder.java`

```java
public final class StatLoggerBuilder extends BaseLoggerBuilder<StatLoggerBuilder> {
    private int intervalSeconds = 60;        // 默认 60s 切换（Sentinel 中改为 1s）
    private int maxEntryCount = 20000;       // 默认 20000（Sentinel 中改为 5000/6000）
    private char keyDelimiter = ',';
    private char valueDelimiter = ',';
    private EagleEyeAppender appender = null;

    StatLogger create() {
        long intervalMillis = TimeUnit.SECONDS.toMillis(this.intervalSeconds);
        String filePath;
        if (this.filePath == null) {
            filePath = EagleEye.EAGLEEYE_LOG_DIR + "stat-" + loggerName + ".log";
        } else if (this.filePath.endsWith("/") || this.filePath.endsWith("\\")) {
            filePath = this.filePath + "stat-" + loggerName + ".log";
        } else {
            filePath = this.filePath;  // Sentinel 走这条
        }
        EagleEyeAppender appender = this.appender;
        if (appender == null) {
            EagleEyeRollingFileAppender rfAppender = new EagleEyeRollingFileAppender(filePath, maxFileSize);
            appender = new SyncAppender(rfAppender);  // 同步包装
        }
        EagleEyeLogDaemon.watch(appender);  // 注册到守护线程
        return new StatLogger(loggerName, appender, intervalMillis, maxEntryCount,
            entryDelimiter, keyDelimiter, valueDelimiter);
    }

    public StatLogger buildSingleton() {
        return StatLogController.createLoggerIfNotExists(this);
    }

    // 切换周期校验：必须能整除 60s，且不超过 5 分钟
    static void validateInterval(final long intervalSeconds) {
        if (intervalSeconds < 1) throw ...;
        else if (intervalSeconds < 60) {
            if (60 % intervalSeconds != 0) throw ...;
        } else if (intervalSeconds <= 5 * 60) {
            if (intervalSeconds % 60 != 0 || 60 % intervalSeconds != 0) throw ...;
        } else {
            throw new IllegalArgumentException("Interval should be less than 5 min");
        }
    }
}
```

### 8.5 `BaseLoggerBuilder` —— 基础参数

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/eagleeye/BaseLoggerBuilder.java`

```java
class BaseLoggerBuilder<T extends BaseLoggerBuilder<T>> {
    protected final String loggerName;
    protected String filePath = null;
    protected long maxFileSize = 1024;        // 默认 1KB（Sentinel 调用 maxFileSizeMB 后覆盖）
    protected char entryDelimiter = '|';
    protected int maxBackupIndex = 3;         // 备份份数（EagleEyeRollingFileAppender 内部硬编码 3，未使用此字段）

    public T configLogFilePath(String filePath) {  // 直接设置绝对路径
        this.filePath = filePath;
        return (T) this;
    }
    public T maxFileSizeMB(long maxFileSizeMB) {    // MB 转 byte
        this.maxFileSize = maxFileSizeMB * 1024 * 1024;
        return (T) this;
    }
    public T maxBackupIndex(int maxBackupIndex) { this.maxBackupIndex = maxBackupIndex; return (T) this; }
    public T entryDelimiter(char entryDelimiter) { this.entryDelimiter = entryDelimiter; return (T) this; }
}
```

### 8.6 `StatLogController` —— 调度与写盘

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/eagleeye/StatLogController.java`

```java
class StatLogController {
    private static final Map<String, StatLogger> statLoggers = new ConcurrentHashMap<>();
    private static final int STAT_ENTRY_COOL_DOWN_MILLIS = 200;  // 200ms cool down 后再写盘

    // 两个独立线程池
    private static final ScheduledThreadPoolExecutor rollerThreadPool =   // 切换数据
        new ScheduledThreadPoolExecutor(1, new NamedThreadFactory(
            "EagleEye-StatLogController-roller", true));
    private static final ScheduledThreadPoolExecutor writerThreadPool =  // 实际写盘
        new ScheduledThreadPoolExecutor(1, new NamedThreadFactory(
            "EagleEye-StatLogController-writer", true));

    static StatLogger createLoggerIfNotExists(StatLoggerBuilder builder) {
        String loggerName = builder.getLoggerName();
        StatLogger statLogger = statLoggers.get(loggerName);
        if (statLogger == null) {
            synchronized (StatLogController.class) {
                if ((statLogger = statLoggers.get(loggerName)) == null) {
                    statLogger = builder.create();
                    statLoggers.put(loggerName, statLogger);
                    writerThreadPool.setMaximumPoolSize(Math.max(1, statLoggers.size()));
                    scheduleNextRollingTask(statLogger);   // 调度下一次 rolling
                }
            }
        }
        return statLogger;
    }

    // 调度 rolling 任务（按 interval 周期触发）
    private static void scheduleNextRollingTask(StatLogger statLogger) {
        if (!running.get()) return;
        StatLogRollingTask rollingTask = new StatLogRollingTask(statLogger);
        long rollingTimeMillis = statLogger.getRollingData().getRollingTimeMillis();
        long delayMillis = rollingTimeMillis - System.currentTimeMillis();
        if (delayMillis > 5) {
            rollerThreadPool.schedule(rollingTask, delayMillis, TimeUnit.MILLISECONDS);
        } else if (-delayMillis > statLogger.getIntervalMillis()) {
            rollerThreadPool.submit(rollingTask);   // 严重延迟，立即执行
        } else {
            rollerThreadPool.submit(rollingTask);
        }
    }

    // 真正写盘任务
    static void scheduleWriteTask(StatRollingData statRollingData) {
        if (statRollingData != null) {
            StatLogWriteTask task = new StatLogWriteTask(statRollingData);
            writerThreadPool.schedule(task, STAT_ENTRY_COOL_DOWN_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    // Rolling 任务：先 rolling 切换数据，再调度写盘任务，再调度下一次 rolling
    private static class StatLogRollingTask implements Runnable {
        public void run() {
            scheduleWriteTask(statLogger.rolling());  // 拿到旧数据并调度写盘
            scheduleNextRollingTask(statLogger);       // 继续下一次 rolling
        }
    }

    // 写盘任务：遍历所有 StatEntry，格式化后追加到文件
    private static class StatLogWriteTask implements Runnable {
        public void run() {
            // 格式：time|statType|keys|values\n
            //   其中 keys 用 keyDelimiter 分隔，values 用 valueDelimiter 分隔
            for (Entry<StatEntry, StatEntryFunc> entry : entrySet) {
                buffer.append(timeStr).append(entryDelimiter);    // 时间
                buffer.append(func.getStatType()).append(entryDelimiter);  // statType
                entry.getKey().appendTo(buffer, keyDelimiter);     // keys
                buffer.append(entryDelimiter);
                func.appendTo(buffer, valueDelimiter);            // values
                buffer.append(EagleEyeCoreUtils.NEWLINE);
                appender.append(buffer.toString());
            }
            appender.flush();
        }
    }
}
```

**关键设计**：
1. **每个 StatLogger 拥有独立的 rolling 调度**，按 interval 周期触发
2. Rolling 时先 CAS 切换 `StatRollingData`，旧的留给 writer 线程写盘
3. **写盘前有 200ms cool down**，给可能稍晚到达的统计事件留时间合并
4. 写盘串行（writerThreadPool core = 1），保证文件写入顺序

### 8.7 `EagleEyeRollingFileAppender` —— 文件追加 + 切分

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/eagleeye/EagleEyeRollingFileAppender.java`

#### 8.7.1 关键字段

```java
class EagleEyeRollingFileAppender extends EagleEyeAppender {
    private static final long LOG_FLUSH_INTERVAL = TimeUnit.SECONDS.toMillis(1);  // 1s flush 一次
    private static final int DEFAULT_BUFFER_SIZE = 4 * 1024;  // 4KB buffer
    private final int maxBackupIndex = 3;                     // 硬编码 3 份
    private final long maxFileSize;
    private final int bufferSize = DEFAULT_BUFFER_SIZE;
    private final String filePath;
    private final AtomicBoolean isRolling = new AtomicBoolean(false);  // 切分中标志
    private BufferedOutputStream bos = null;
    private long nextFlushTime = 0L;
    private long lastRollOverTime = 0L;
    private long outputByteSize = 0L;                         // 当前文件已写字节数
    private final boolean selfLogEnabled;
    private boolean multiProcessDetected = false;             // 多进程写入检测
    private static final String DELETE_FILE_SUFFIX = ".deleted";  // 标记待删除的备份
}
```

#### 8.7.2 `setFile()` —— 打开/创建文件

```java
private void setFile() {
    try {
        File logFile = new File(filePath);
        if (!logFile.exists()) {
            File parentFile = logFile.getParentFile();
            if (!parentFile.exists() && !parentFile.mkdirs()) { ... }
            if (!logFile.createNewFile()) { ... }   // 创建新文件
        }
        if (!logFile.isFile() || !logFile.canWrite()) { ... }
        FileOutputStream ostream = new FileOutputStream(logFile, true);  // O_APPEND 追加
        this.bos = new BufferedOutputStream(ostream, bufferSize);
        this.lastRollOverTime = System.currentTimeMillis();
        this.outputByteSize = logFile.length();     // 已有内容大小
    } catch (Throwable e) { ... }
}
```

#### 8.7.3 `append()` —— 写入 + 大小检查

```java
@Override
public void append(String log) {
    BufferedOutputStream bos = this.bos;
    if (bos != null) {
        try {
            waitUntilRollFinish();   // 等待正在进行的切分完成
            byte[] bytes = log.getBytes(EagleEye.DEFAULT_CHARSET);  // GB18030
            int len = bytes.length;
            if (len > DEFAULT_BUFFER_SIZE && this.multiProcessDetected) {
                len = DEFAULT_BUFFER_SIZE;
                bytes[len - 1] = '\n';
            }
            bos.write(bytes, 0, len);
            outputByteSize += len;
            if (outputByteSize >= maxFileSize) {
                rollOver();           // 超过大小，切分
            } else {
                if (System.currentTimeMillis() >= nextFlushTime) {
                    flush();          // 1 秒 flush 一次
                }
            }
        } catch (Exception e) {
            doSelfLog("[ERROR] fail to write log to file " + filePath + ", error=" + e.getMessage());
            close();
            setFile();                // 出错重开文件
        }
    }
}
```

#### 8.7.4 `rollOver()` —— 文件切分（按大小）

```java
@Override
public void rollOver() {
    final String lockFilePath = filePath + ".lock";
    final File lockFile = new File(lockFilePath);
    if (!isRolling.compareAndSet(false, true)) return;  // 防重入

    try (RandomAccessFile raf = new RandomAccessFile(lockFile, "rw");
         FileLock fileLock = raf.getChannel().tryLock()) {   // 文件锁，跨进程互斥
        if (fileLock != null) {
            reload();                                         // 先 reload 检查文件状态
            if (outputByteSize >= maxFileSize) {
                // 删除最老的备份（maxBackupIndex 号），重命名为 .deleted
                file = new File(filePath + '.' + maxBackupIndex);
                if (file.exists()) {
                    target = new File(filePath + '.' + maxBackupIndex + DELETE_FILE_SUFFIX);
                    file.renameTo(target) 或 file.delete();
                }
                // 从 maxBackupIndex-1 到 1，依次重命名为 +1
                for (int i = maxBackupIndex - 1; i >= 1; i--) {
                    file = new File(filePath + '.' + i);
                    if (file.exists()) file.renameTo(new File(filePath + '.' + (i + 1)));
                }
                // 当前文件重命名为 .1
                close();
                file = new File(filePath);
                file.renameTo(new File(filePath + "." + 1));
                setFile();   // 重新创建空文件
            }
        }
    } finally {
        isRolling.set(false);
        // 释放文件锁、关闭 RandomAccessFile、删除 lock 文件
    }
}
```

#### 8.7.5 `reload()` —— 多进程检测

```java
@Override
public void reload() {
    flush();
    File logFile = new File(filePath);
    long fileSize = logFile.length();
    boolean fileNotExists = fileSize <= 0 && !logFile.exists();

    if (this.bos == null || fileSize < outputByteSize || fileNotExists) {
        // 文件被外部删除或截断，强制重开
        close();
        setFile();
    } else if (fileSize > outputByteSize) {
        // 文件比预期大 → 有别的进程在写！
        this.outputByteSize = fileSize;
        if (!this.multiProcessDetected) {
            this.multiProcessDetected = true;
            doSelfLog("[WARN] Multi-process file write detected: " + filePath);
        }
    }
}
```

#### 8.7.6 `cleanup()` —— 清理 .deleted 文件

```java
@Override
public void cleanup() {
    // 扫描父目录，找出所有 baseFileName*.deleted 的文件并删除
    File[] filesToDelete = parentDir.listFiles((dir, name) ->
        name != null && name.startsWith(baseFileName) && name.endsWith(DELETE_FILE_SUFFIX));
    for (File f : filesToDelete) f.delete() 或跳过;
}
```

### 8.8 `EagleEyeLogDaemon` —— 后台守护线程

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/eagleeye/EagleEyeLogDaemon.java`

```java
class EagleEyeLogDaemon implements Runnable {
    private static final long LOG_CHECK_INTERVAL = TimeUnit.SECONDS.toMillis(20);  // 20s 检查一次
    private static AtomicBoolean running = new AtomicBoolean(false);
    private static Thread worker = null;
    private static final CopyOnWriteArrayList<EagleEyeAppender> watchedAppenders = new CopyOnWriteArrayList<>();

    static EagleEyeAppender watch(EagleEyeAppender appender) {
        watchedAppenders.addIfAbsent(appender);
        return appender;
    }

    @Override
    public void run() {
        while (running.get()) {
            cleanupFiles();            // 1. 清理 .deleted 文件
            Thread.sleep(LOG_CHECK_INTERVAL);  // 20s
            flushAndReload();           // 2. flush + reload，检测多进程
        }
    }

    static void start() {
        if (running.compareAndSet(false, true)) {
            Thread worker = new Thread(new EagleEyeLogDaemon());
            worker.setDaemon(true);                     // daemon 线程
            worker.setName("EagleEye-LogDaemon-Thread");
            worker.start();
        }
    }
}
```

### 8.9 `SyncAppender` —— 同步包装

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/eagleeye/SyncAppender.java

```java
final class SyncAppender extends EagleEyeAppender {
    private final EagleEyeAppender delegate;
    private final Object lock = new Object();

    @Override
    public void append(String log) {
        synchronized (lock) { delegate.append(log); }   // 串行化
    }
    // flush/rollOver/reload/close 全部 synchronized
}
```

**作用**：保证多线程对同一文件的写入串行化，避免内容交错。

### 8.10 `TokenBucket` —— 异常日志限流

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/eagleeye/TokenBucket.java`

`EagleEye.selfLog` 使用令牌桶限流异常日志（10 秒 10 个），避免异常日志爆量。

---

## 九、EagleEyeLogUtil —— sentinel-block.log 限流日志

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/logger/EagleEyeLogUtil.java`

### 9.1 类定义

```java
public class EagleEyeLogUtil {
    public static final String FILE_NAME = "sentinel-block.log";

    private static StatLogger statLogger;

    static {
        String path = LogBase.getLogBaseDir() + FILE_NAME;   // ${user.home}/logs/csp/sentinel-block.log
        statLogger = EagleEye.statLoggerBuilder("sentinel-block-log")
            .intervalSeconds(1)                  // 1 秒切换一次
            .entryDelimiter('|')                 // 条目分隔符
            .keyDelimiter(',')                   // key 内分隔符
            .valueDelimiter(',')                 // value 内分隔符
            .maxEntryCount(6000)                 // 单窗口最多 6000 条
            .configLogFilePath(path)             // 文件路径
            .maxFileSizeMB(300)                  // 300MB
            .maxBackupIndex(3)                   // 3 份备份（注：实际硬编码 3）
            .buildSingleton();
    }

    public static void log(String resource, String exceptionName, String ruleLimitApp, String origin, Long ruleId, int count) {
        String ruleIdString = StringUtil.EMPTY;
        if (ruleId != null) {
            ruleIdString = String.valueOf(ruleId);
        }
        statLogger.stat(resource, exceptionName, ruleLimitApp, origin, ruleIdString).count(count);
    }
}
```

### 9.2 数据来源

**唯一调用方**：`LogSlot.entry()`（见 [第十二节](#十二logslot--限流拦截与日志记录)）。

```java
// LogSlot.java
catch (BlockException e) {
    EagleEyeLogUtil.log(resourceWrapper.getName(), e.getClass().getSimpleName(), e.getRuleLimitApp(),
        context.getOrigin(), e.getRule() != null ? e.getRule().getId() : null, count);
    throw e;
}
```

### 9.3 日志格式

由 `StatLogWriteTask` 格式化：
```
time|statType|keys|values\n
```
其中 keys = `resource,exceptionName,ruleLimitApp,origin,ruleId`，values = `count`。

**实际样例**：
```
2026-08-12 14:30:25|sentinel-block-log|com.example.HelloService,FlowException,default,appA,1|1
2026-08-12 14:30:25|sentinel-block-log|com.example.HelloService,FlowException,default,appA,1|3
2026-08-12 14:30:26|sentinel-block-log|/api/order,DegradeException,default,appB,5|2
```

> 同一秒内的相同 key 会被合并（StatEntryFunc 累加 count），不会逐条记录。这是 EagleEye 的核心价值：**统计聚合**，而非逐条审计。

### 9.4 文件切分策略

- **按时间**：1 秒切换 StatRollingData（不切文件，只是切换内存累计桶）
- **按大小**：超过 300MB 触发 `rollOver`，重命名为 `.1`、`.2`、`.3`，最老的删除
- **备份**：3 份（硬编码在 `EagleEyeRollingFileAppender.maxBackupIndex = 3`）

---

## 十、ClusterStatLogUtil 系列 —— 集群限流日志

Sentinel 集群限流相关统计日志共有 3 个，全部使用 EagleEye 框架，配置参数几乎一致。

### 10.1 `ClusterStatLogUtil` —— 集群通用统计

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/cluster/log/ClusterStatLogUtil.java`

```java
public final class ClusterStatLogUtil {
    private static final String FILE_NAME = "sentinel-cluster.log";

    static {
        String path = LogBase.getLogBaseDir() + FILE_NAME;
        statLogger = EagleEye.statLoggerBuilder("sentinel-cluster-record")
            .intervalSeconds(1)
            .entryDelimiter('|').keyDelimiter(',').valueDelimiter(',')
            .maxEntryCount(5000)
            .configLogFilePath(path)
            .maxFileSizeMB(300)
            .maxBackupIndex(3)
            .buildSingleton();
    }

    public static void log(String msg) { statLogger.stat(msg).count(); }
    public static void log(String msg, int count) { statLogger.stat(msg).count(count); }
}
```

**数据来源**：`ClusterSlot`（集群限流判断 Slot）记录的 token 请求/响应结果。

### 10.2 `ClusterClientStatLogUtil` —— 集群客户端统计

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/cluster/log/ClusterClientStatLogUtil.java`

```java
public final class ClusterClientStatLogUtil {
    private static final String FILE_NAME = "sentinel-cluster-client.log";
    // ... 配置同上（5000 entry、300MB、3 备份）
}
```

**数据来源**：`DefaultClusterTokenClient` 在请求 token 后记录结果：
- `cluster_request|success|<flowId>|<token>`
- `cluster_request|blocked|<flowId>|<reason>`
- `cluster_request|error|<flowId>|<error>`

### 10.3 `ClusterServerStatLogUtil` —— 集群服务端统计

**文件**：`sentinel-cluster/sentinel-cluster-server-default/src/main/java/com/alibaba/csp/sentinel/cluster/server/log/ClusterServerStatLogUtil.java`

```java
public final class ClusterServerStatLogUtil {
    private static final String FILE_NAME = "sentinel-server.log";
    // ... 配置同上
}
```

**数据来源**：`TokenServerHandler` 在处理 token 请求后记录结果：
- `cluster_flow_request|success|<namespace>`
- `cluster_flow_request|blocked|<namespace>|<reason>`
- `cluster_param_request|success|<namespace>`

### 10.4 三个文件的区别

| 文件 | 模块 | 写入方 | 角色 |
| --- | --- | --- | --- |
| `sentinel-cluster.log` | sentinel-core | `ClusterStatLogUtil` | 集群限流 Slot 记录客户端调用结果（汇总） |
| `sentinel-cluster-client.log` | sentinel-core | `ClusterClientStatLogUtil` | 客户端记录每次 token 请求明细 |
| `sentinel-server.log` | sentinel-cluster-server-default | `ClusterServerStatLogUtil` | 服务端记录每次 token 请求处理结果 |

---

## 十一、MetricWriter —— metrics.log 指标日志

Sentinel 的"秒级监控数据持久化"，由 `MetricTimerListener` 定时拉取并落盘。

### 11.1 `MetricTimerListener` —— 触发器

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/node/metric/MetricTimerListener.java

```java
public class MetricTimerListener implements Runnable {
    private static final MetricWriter metricWriter = new MetricWriter(
        SentinelConfig.singleMetricFileSize(),      // 默认 50MB
        SentinelConfig.totalMetricFileCount());     // 默认 6 份

    @Override
    public void run() {
        Map<Long, List<MetricNode>> maps = new TreeMap<>();
        // 遍历所有 ClusterNode，拉取每秒指标
        for (Entry<ResourceWrapper, ClusterNode> e : ClusterBuilderSlot.getClusterNodeMap().entrySet()) {
            ClusterNode node = e.getValue();
            Map<Long, MetricNode> metrics = node.metrics();   // 滚动窗口的秒级数据
            aggregate(maps, metrics, node);
        }
        aggregate(maps, Constants.ENTRY_NODE.metrics(), Constants.ENTRY_NODE);  // 入口节点
        if (!maps.isEmpty()) {
            for (Entry<Long, List<MetricNode>> entry : maps.entrySet()) {
                try {
                    metricWriter.write(entry.getKey(), entry.getValue());   // 写盘
                } catch (Exception e) {
                    RecordLog.warn("[MetricTimerListener] Write metric error", e);
                }
            }
        }
    }
}
```

`MetricTimerListener` 由 `InitExecutor` / `Constants` 注册到定时器，默认每 **1 秒** 执行一次（由 `csp.sentinel.metric.flush.interval` 控制）。

### 11.2 `MetricWriter` —— 写入实现

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/node/metric/MetricWriter.java`

#### 11.2.1 关键常量与字段

```java
public class MetricWriter {
    private static final String CHARSET = SentinelConfig.charset();
    public static final String METRIC_BASE_DIR = LogBase.getLogBaseDir();   // 同 RecordLog 路径
    public static final String METRIC_FILE = "metrics.log";
    public static final String METRIC_FILE_INDEX_SUFFIX = ".idx";

    private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private long timeSecondBase;     // 1970-01-01 00:00:00 的秒数，用于 isNewDay 判断
    private String baseDir;
    private String baseFileName;     // 例如 "app-metrics.log.pid1234"
    private File curMetricFile;
    private File curMetricIndexFile;
    private FileOutputStream outMetric;
    private DataOutputStream outIndex;
    private BufferedOutputStream outMetricBuf;
    private long singleFileSize;
    private int totalFileCount;
    private boolean append = false;
    private final int pid = PidUtil.getPid();
    private long lastSecond = -1;
}
```

#### 11.2.2 构造器

```java
public MetricWriter(long singleFileSize, int totalFileCount) {
    if (singleFileSize <= 0 || totalFileCount <= 0) throw new IllegalArgumentException();
    RecordLog.info("[MetricWriter] Creating new MetricWriter, singleFileSize={}, totalFileCount={}",
        singleFileSize, totalFileCount);
    this.baseDir = METRIC_BASE_DIR;
    File dir = new File(baseDir);
    if (!dir.exists()) dir.mkdirs();
    // ...
    this.singleFileSize = singleFileSize;       // 默认 50MB
    this.totalFileCount = totalFileCount;       // 默认 6
    this.timeSecondBase = df.parse("1970-01-01 00:00:00").getTime() / 1000;
}
```

#### 11.2.3 文件名格式：`formMetricFileName`

```java
public static String formMetricFileName(String appName, int pid) {
    if (appName == null) appName = "";
    final String dot = ".";
    final String separator = "-";
    if (appName.contains(dot)) appName = appName.replace(dot, separator);   // appName 中的 . 替换为 -
    String name = appName + separator + METRIC_FILE;                         // 例如 "my-app-metrics.log"
    if (LogBase.isLogNameUsePid()) {
        name += ".pid" + pid;                                                 // 可选 pid
    }
    return name;
}
```

**最终文件名样例**：
- 默认（无 pid）：`-metrics.log.2026-08-12.1`、`-metrics.log.2026-08-12.2`
- 带 pid：`my-app-metrics.log.pid12345.2026-08-12.1`
- 索引文件：`-metrics.log.2026-08-12.1.idx`

#### 11.2.4 `write()` —— 写入逻辑

```java
public synchronized void write(long time, List<MetricNode> nodes) throws Exception {
    if (nodes == null) return;
    for (MetricNode node : nodes) node.setTimestamp(time);

    String appName = SentinelConfig.getAppName();
    if (appName == null) appName = "";
    if (curMetricFile == null) {                           // 首次写入
        baseFileName = formMetricFileName(appName, pid);
        closeAndNewFile(nextFileNameOfDay(time));         // 创建当天第一个文件
    }
    if (!(curMetricFile.exists() && curMetricIndexFile.exists())) {
        closeAndNewFile(nextFileNameOfDay(time));         // 文件被外部删除，重建
    }

    long second = time / 1000;
    if (second < lastSecond) {
        // 时间靠前的直接忽略，不应该发生。
    } else if (second == lastSecond) {
        for (MetricNode node : nodes) {
            outMetricBuf.write(node.toFatString().getBytes(CHARSET));
        }
        outMetricBuf.flush();
        if (!validSize()) {
            closeAndNewFile(nextFileNameOfDay(time));     // 超大小，新建
        }
    } else {
        writeIndex(second, outMetric.getChannel().position());  // 写索引
        if (isNewDay(lastSecond, second)) {
            closeAndNewFile(nextFileNameOfDay(time));     // 跨天，新建
        }
        for (MetricNode node : nodes) {
            outMetricBuf.write(node.toFatString().getBytes(CHARSET));
        }
        outMetricBuf.flush();
        if (!validSize()) {
            closeAndNewFile(nextFileNameOfDay(time));     // 超大小，新建
        }
        lastSecond = second;
    }
}
```

#### 11.2.5 `nextFileNameOfDay()` —— 计算下一个文件名

```java
private String nextFileNameOfDay(long time) {
    List<String> list = new ArrayList<>();
    File baseFile = new File(baseDir);
    DateFormat fileNameDf = new SimpleDateFormat("yyyy-MM-dd");
    String dateStr = fileNameDf.format(new Date(time));
    String fileNameModel = baseFileName + "." + dateStr;    // 例如 "my-app-metrics.log.2026-08-12"
    for (File file : baseFile.listFiles()) {
        String fileName = file.getName();
        if (fileName.contains(fileNameModel)
            && !fileName.endsWith(METRIC_FILE_INDEX_SUFFIX)
            && !fileName.endsWith(".lck")) {
            list.add(file.getAbsolutePath());
        }
    }
    Collections.sort(list, METRIC_FILE_NAME_CMP);
    if (list.isEmpty()) {
        return baseDir + fileNameModel;                     // 第 1 个文件，无后缀
    }
    String last = list.get(list.size() - 1);                 // 今天的最后一个文件
    int n = 0;
    String[] strs = last.split("\\.");
    if (strs.length > 0 && strs[strs.length - 1].matches("[0-9]{1,10}")) {
        n = Integer.parseInt(strs[strs.length - 1]);
    }
    return baseDir + fileNameModel + "." + (n + 1);          // 编号 +1
}
```

**结果**：每天的第 1 个文件是 `appName-metrics.log.yyyy-MM-dd`（无后缀），第 2 个是 `.1`、第 3 个是 `.2`...

#### 11.2.6 `closeAndNewFile()` 与 `removeMoreFiles()`

```java
private void closeAndNewFile(String fileName) throws Exception {
    removeMoreFiles();                       // 先清理超额的旧文件
    if (outMetricBuf != null) outMetricBuf.close();
    if (outIndex != null) outIndex.close();
    outMetric = new FileOutputStream(fileName, append);
    outMetricBuf = new BufferedOutputStream(outMetric);
    curMetricFile = new File(fileName);
    String idxFile = formIndexFileName(fileName);
    curMetricIndexFile = new File(idxFile);
    outIndex = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(idxFile, append)));
    RecordLog.info("[MetricWriter] New metric file created: {}", fileName);
    RecordLog.info("[MetricWriter] New metric index file created: {}", idxFile);
}

private void removeMoreFiles() throws Exception {
    List<String> list = listMetricFiles(baseDir, baseFileName);
    if (list == null || list.isEmpty()) return;
    // 总数超过 totalFileCount，删除最老的
    for (int i = 0; i < list.size() - totalFileCount + 1; i++) {
        String fileName = list.get(i);
        String indexFile = formIndexFileName(fileName);
        new File(fileName).delete();
        RecordLog.info("[MetricWriter] Removing metric file: {}", fileName);
        new File(indexFile).delete();
        RecordLog.info("[MetricWriter] Removing metric index file: {}", indexFile);
    }
}
```

#### 11.2.7 `writeIndex()` —— 索引文件

```java
private void writeIndex(long time, long offset) throws Exception {
    outIndex.writeLong(time);     // 8 字节时间戳（秒）
    outIndex.writeLong(offset);   // 8 字节文件偏移量
    outIndex.flush();
}
```

**索引文件作用**：`MetricSearcher` 可根据时间快速定位到指标文件中的位置，无需全文件扫描。

#### 11.2.8 `MetricNode.toFatString()` —— 数据格式

`MetricNode` 的 `toFatString()` 输出以换行分隔的多行字段：
```
resource
classification
timestamp
passQps
blockQps
successQps
exceptionQps
rt
occupiedPassQps
concurrency
...
```

### 11.3 配置项

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/config/SentinelConfig.java`

```java
public static final String SINGLE_METRIC_FILE_SIZE = "csp.sentinel.metric.file.single.size";
public static final String TOTAL_METRIC_FILE_COUNT = "csp.sentinel.metric.file.total.count";
public static final String METRIC_FLUSH_INTERVAL = "csp.sentinel.metric.flush.interval";

public static final long DEFAULT_SINGLE_METRIC_FILE_SIZE = 1024 * 1024 * 50;   // 50MB
public static final int DEFAULT_TOTAL_METRIC_FILE_COUNT = 6;
public static final long DEFAULT_METRIC_FLUSH_INTERVAL = 1L;                     // 1 秒
```

### 11.4 数据来源总结

| 字段 | 来源 |
| --- | --- |
| `resource` | `ClusterNode.getName()`（ResourceWrapper） |
| `classification` | `ClusterNode.getResourceType()` |
| `passQps` / `blockQps` / `successQps` / `exceptionQps` | `ArrayMetric` → `MetricBucket`（滑动窗口统计） |
| `rt` | 平均响应时间 |
| `occupiedPassQps` | `OccupiableBucketLeapArray` 借用未来 token 的统计 |
| `concurrency` | 当前并发线程数 |

数据流：
```
Slot Chain 中的 StatisticSlot → ClusterNode.metric() → MetricTimerListener.run() → MetricWriter.write() → metrics.log
```

---

## 十二、LogSlot —— 限流拦截与日志记录

**文件**：`sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/logger/LogSlot.java`

### 12.1 Slot 位置

在 Slot Chain 中的顺序：

```
NodeSelectorSlot(-10000) → ClusterBuilderSlot(-9000) → LogSlot(-8000) → StatisticSlot(-7000) → AuthoritySlot(-6000) → SystemSlot(-5000) → FlowSlot(-2000) → DefaultCircuitBreakerSlot(-1500) → DegradeSlot(-1000)
```

`LogSlot` 排在 `StatisticSlot` 之前，先包装 `fireEntry`，捕获后续 Slot（FlowSlot、DegradeSlot 等）抛出的 `BlockException`。

### 12.2 源码

```java
@Spi(order = Constants.ORDER_LOG_SLOT)   // -8000
public class LogSlot extends AbstractLinkedProcessorSlot<DefaultNode> {

    @Override
    public void entry(Context context, ResourceWrapper resourceWrapper, DefaultNode obj, int count, boolean prioritized, Object... args)
        throws Throwable {
        try {
            fireEntry(context, resourceWrapper, obj, count, prioritized, args);  // 先放行后续 Slot
        } catch (BlockException e) {
            // 记录限流/熔断日志到 sentinel-block.log
            EagleEyeLogUtil.log(resourceWrapper.getName(), e.getClass().getSimpleName(), e.getRuleLimitApp(),
                context.getOrigin(), e.getRule() != null ? e.getRule().getId() : null, count);
            throw e;   // 继续向上抛
        } catch (Throwable e) {
            RecordLog.warn("Unexpected entry exception", e);   // 非 BlockException，记到 RecordLog
        }
    }

    @Override
    public void exit(Context context, ResourceWrapper resourceWrapper, int count, Object... args) {
        try {
            fireExit(context, resourceWrapper, count, args);
        } catch (Throwable e) {
            RecordLog.warn("Unexpected entry exit exception", e);
        }
    }
}
```

### 12.3 触发流程

```
CtSph.entry() → Slot Chain entry()
   → NodeSelectorSlot.entry → fireEntry
   → ClusterBuilderSlot.entry → fireEntry
   → LogSlot.entry → try {
       fireEntry → StatisticSlot → AuthoritySlot → SystemSlot → FlowSlot
                                                            ↓ 抛 BlockException
       FlowException 沿调用链向上抛
     } catch (BlockException e) {
        EagleEyeLogUtil.log(...)  ← 此处记录到 sentinel-block.log
        throw e;
     }
```

**注意**：`LogSlot` 是 `sentinel-block.log` 的**唯一数据源**，每次发生 BlockException 都会调用 `EagleEyeLogUtil.log`，但 EagleEye 内部会按 1 秒窗口聚合同一 key（resource + exception + ruleLimitApp + origin + ruleId），所以日志文件中看到的是聚合后的统计。

---

## 十三、各模块日志输出全景

### 13.1 sentinel-core 模块

| 类 | 日志类型 | 触发场景 |
| --- | --- | --- |
| `LogBase` | System.out（启动时） | 日志初始化信息 |
| `LogConfigLoader` | System.err | 配置加载失败 |
| `LoggerSpiProvider` | System.out | SPI 加载结果 |
| `RecordLog` | RecordLog | 静态初始化失败 |
| `SentinelConfig` | RecordLog | 配置加载、解析失败 |
| `DefaultSlotChainBuilder` | RecordLog | Slot Chain 构建异常 |
| `CtSph` | RecordLog | 资源 entry 异常、chain 复用 |
| `LogSlot` | EagleEyeLogUtil + RecordLog | BlockException → block.log；其他异常 → record.log |
| `StatisticSlot` | RecordLog | 统计异常 |
| `FlowSlot` | RecordLog | 规则匹配异常 |
| `DegradeSlot` | RecordLog | 熔断状态切换 |
| `DefaultCircuitBreakerSlot` | RecordLog | 熔断状态切换 |
| `SystemSlot` | RecordLog | 系统规则校验 |
| `AuthoritySlot` | RecordLog | 授权校验 |
| `MetricWriter` | RecordLog | 文件创建、删除 |
| `MetricTimerListener` | RecordLog | 写入失败 |
| `EagleEye` | selfLog（eagleeye-self.log） | EagleEye 启动、异常 |
| `EagleEyeLogUtil` | EagleEye（sentinel-block.log） | BlockException |
| `ClusterStatLogUtil` | EagleEye（sentinel-cluster.log） | 集群限流统计 |
| `ClusterClientStatLogUtil` | EagleEye（sentinel-cluster-client.log） | 集群客户端统计 |
| `InitExecutor` | RecordLog | 初始化函数执行 |

### 13.2 sentinel-transport 模块

| 类 | 日志类型 | 触发场景 |
| --- | --- | --- |
| `SimpleHttpCommandCenter` | CommandCenterLog | HTTP 服务启动、端口绑定 |
| `HttpEventTask`（simple-http） | CommandCenterLog | Socket 接入、请求处理 |
| `SimpleHttpHeartbeatSender` | RecordLog | 心跳发送、控制台地址解析 |
| `NettyHttpCommandCenter` | CommandCenterLog | Netty 服务启动 |
| `HttpServer` | CommandCenterLog + RecordLog | bind 重试、端口冲突 |
| `HttpServerHandler`（netty） | CommandCenterLog | 请求处理异常 |
| `HttpHeartbeatSender`（netty） | RecordLog | 心跳发送、失败 |
| `HeartbeatSenderInitFunc` | RecordLog | 心跳初始化、调度 |
| `TransportConfig` | RecordLog | 端口、IP 配置 |
| `AppNameUtil` | RecordLog | AppName 解析 |
| 各 `CommandHandler` 实现 | RecordLog | 命令处理日志 |

### 13.3 sentinel-cluster 模块

| 类 | 日志类型 | 触发场景 |
| --- | --- | --- |
| `DefaultClusterTokenClient` | RecordLog + ClusterClientStatLogUtil | 客户端创建、连接、token 请求 |
| `TokenClientHandler` | RecordLog + ClusterClientStatLogUtil | 连接事件、异常 |
| `NettyTransportServer` | RecordLog | 服务端启动、停止 |
| `TokenServerHandler` | RecordLog + ClusterServerStatLogUtil | 请求处理 |
| `ConnectionManager` | RecordLog | 客户端连接、断开 |
| `ClusterFlowRuleManager` | RecordLog | 规则加载、变更 |

### 13.4 sentinel-extension 模块（动态数据源）

| 类 | 日志类型 | 触发场景 |
| --- | --- | --- |
| `FileRefreshableDataSource` | RecordLog | 文件不存在、读取异常 |
| `NacosDataSource` | RecordLog | Nacos 配置变更、加载失败 |
| `ApolloDataSource` | RecordLog | Apollo 配置变更 |
| `ZookeeperDataSource` | RecordLog | ZK 节点变更、连接失败 |
| `RedisDataSource` | RecordLog | Redis 频道消息 |
| `ConsulDataSource` | RecordLog | Consul KV 变更 |
| `EtcdDataSource` | RecordLog | Etcd 键变更 |
| `AutoRefreshDataSource` | RecordLog | 定时刷新 |

### 13.5 sentinel-adapter 模块

| 适配器 | 日志类型 | 触发场景 |
| --- | --- | --- |
| `SentinelApacheDubboProviderFilter` | RecordLog | 过滤器初始化 |
| `SentinelApacheDubboConsumerFilter` | RecordLog | 过滤器初始化 |
| `SentinelAlibabaDubboProviderFilter` | RecordLog | 过滤器初始化 |
| `SentinelDubboConsumerFilter` | RecordLog | 过滤器初始化 |
| Spring Web 适配器 | RecordLog | 适配器初始化 |

---

## 十四、日志落盘路径汇总

### 14.1 默认路径

所有 Sentinel 日志默认都位于 `${user.home}/logs/csp/`，例如 Linux 下 `/root/logs/csp/`，Windows 下 `C:\Users\<user>\logs\csp\`。

| 文件 | 完整路径（默认） | 写入类 |
| --- | --- | --- |
| 业务日志 | `${user.home}/logs/csp/sentinel-record.yyyy-MM-dd.N` | `DateFileLogHandler` |
| 命令中心日志 | `${user.home}/logs/csp/command-center.yyyy-MM-dd.N` | `DateFileLogHandler` |
| 限流日志 | `${user.home}/logs/csp/sentinel-block.log.N` | `EagleEyeRollingFileAppender` |
| 集群通用日志 | `${user.home}/logs/csp/sentinel-cluster.log.N` | `EagleEyeRollingFileAppender` |
| 集群客户端日志 | `${user.home}/logs/csp/sentinel-cluster-client.log.N` | `EagleEyeRollingFileAppender` |
| 集群服务端日志 | `${user.home}/logs/csp/sentinel-server.log.N` | `EagleEyeRollingFileAppender` |
| 指标日志 | `${user.home}/logs/csp/${appName}-metrics.log.yyyy-MM-dd.N` | `MetricWriter` |
| 指标索引 | `${user.home}/logs/csp/${appName}-metrics.log.yyyy-MM-dd.N.idx` | `MetricWriter` |
| EagleEye 自身 | `${user.home}/logs/eagleeye/eagleeye-self.log.N` | `EagleEyeRollingFileAppender`（直接） |

### 14.2 自定义路径

通过 `-Dcsp.sentinel.log.dir=/your/path/` 即可重定向所有 Sentinel 日志到指定目录（除了 `eagleeye-self.log`，它走 `EAGLEEYE.LOG.PATH` 或 `JM.LOG.PATH`）。

```bash
java -Dcsp.sentinel.log.dir=/var/log/sentinel/ -jar your-app.jar
```

效果：
- `sentinel-record.log` → `/var/log/sentinel/sentinel-record.yyyy-MM-dd.N`
- `sentinel-block.log` → `/var/log/sentinel/sentinel-block.log.N`
- `metrics.log` → `/var/log/sentinel/${appName}-metrics.log.yyyy-MM-dd.N`
- 但 `eagleeye-self.log` 仍在 `${user.home}/logs/eagleeye/`（除非也配置 `EAGLEEYE.LOG.PATH`）

### 14.3 文件名带 PID

通过 `-Dcsp.sentinel.log.use.pid=true` 启用：

```bash
java -Dcsp.sentinel.log.use.pid=true -jar your-app.jar
```

效果（假设 pid=12345）：
- `sentinel-record.yyyy-MM-dd.N.pid12345`（注意：实际 DateFileLogHandler 拼接位置在文件名后，%d 前）
- `appName-metrics.log.pid12345.yyyy-MM-dd.N`
- 但 EagleEye 的 block/cluster 日志**不**带 pid（EagleEyeLogUtil 中没有 pid 拼接逻辑）

### 14.4 配置文件路径

通过 `-Dcsp.sentinel.config.file=/path/to/sentinel.properties` 或环境变量 `CSP_SENTINEL_CONFIG_FILE` 指定配置文件，或使用默认 `classpath:sentinel.properties`。

---

## 十五、日志文件存储策略

### 15.1 RecordLog / CommandCenterLog（JUL 实现的策略）

| 维度 | 配置 | 说明 |
| --- | --- | --- |
| 单文件大小上限 | **200MB**（硬编码） | `1024 * 1024 * 200` |
| 备份份数 | **4**（硬编码） | 写满后保留 4 个历史文件 |
| 日期切分 | 每天 0 点 | `DateFileLogHandler.rotateDate()` |
| 大小切分 | 超过 200MB | 由 JUL `FileHandler` 内部处理 |
| 文件名格式 | `sentinel-record.yyyy-MM-dd.N` | N 从 0 到 count-1 |
| 追加模式 | `append=true` | 文件存在则追加 |
| 字符集 | UTF-8（默认） | `csp.sentinel.log.charset` 可配置 |
| 写入方式 | 异步（线程池 + ArrayBlockingQueue(1024)） | 拒绝策略：DiscardOldestPolicy |
| 写入失败处理 | 丢弃最老任务，60s 打印一次 stderr | `LogRejectedExecutionHandler` |

**注意**：JUL `FileHandler` 实际生成的文件名格式与 pattern 略有差异，例如 pattern `sentinel-record.%d` 会被替换为 `sentinel-record.2026-08-12`，再按大小切分为 `sentinel-record.2026-08-12.0`、`.1`、`.2`、`.3`。

### 15.2 EagleEye 系列日志（block / cluster / server）

| 维度 | 配置 | 说明 |
| --- | --- | --- |
| 单文件大小上限 | **300MB** | `.maxFileSizeMB(300)` |
| 备份份数 | **3**（硬编码） | `EagleEyeRollingFileAppender.maxBackupIndex = 3`（注：Builder 中的 maxBackupIndex 配置实际未被使用） |
| 时间切分 | **不按日期切分文件**，只按时间窗口聚合内存数据 | `intervalSeconds(1)` 仅切换 StatRollingData |
| 大小切分 | 超过 300MB 触发 `rollOver` | `EagleEyeRollingFileAppender.rollOver()` |
| 文件名格式 | `sentinel-block.log.N`（N=1,2,3） | 注意：**不含日期** |
| 追加模式 | `O_APPEND` | `new FileOutputStream(logFile, true)` |
| 字符集 | **GB18030**（默认） | `EagleEye.DEFAULT_CHARSET` |
| 写入方式 | 同步串行（SyncAppender + synchronized） | 通过 `StatLogController.writerThreadPool` 异步触发 |
| 切分时文件锁 | `filePath + ".lock"` + `FileLock` | 跨进程互斥 |
| 备份删除 | 重命名为 `.deleted`，由 daemon 线程 20s 清理 | `cleanup()` |
| 多进程检测 | 检测到外部写入会截断到 4KB 并打 warn 日志 | `multiProcessDetected` |

**滚动规则**：
1. 写入超过 maxFileSize → 调用 `rollOver()`
2. 删除 `sentinel-block.log.3`（重命名为 `.deleted`）
3. `sentinel-block.log.2` → `sentinel-block.log.3`
4. `sentinel-block.log.1` → `sentinel-block.log.2`
5. `sentinel-block.log` → `sentinel-block.log.1`
6. 创建新的 `sentinel-block.log`
7. Daemon 线程每 20s 清理 `.deleted` 文件

### 15.3 MetricWriter（指标日志）

| 维度 | 配置 | 说明 |
| --- | --- | --- |
| 单文件大小上限 | **50MB**（默认） | `csp.sentinel.metric.file.single.size` |
| 备份份数 | **6**（默认） | `csp.sentinel.metric.file.total.count` |
| 写入周期 | **1 秒** | `csp.sentinel.metric.flush.interval` |
| 日期切分 | 跨天即新建文件 | `isNewDay(lastSecond, second)` |
| 大小切分 | 超过 singleFileSize 新建 | `validSize()` |
| 文件名格式 | `${appName}-metrics.log.yyyy-MM-dd[.N]` | 第 1 个无 N，第 2 个起带 `.1`、`.2` |
| 索引文件 | 与指标文件配对（`.idx` 后缀） | 8 字节时间戳 + 8 字节偏移量 |
| 追加模式 | `append=false`（构造器默认） | 但首次创建后保持 |
| 字符集 | `SentinelConfig.charset()`（默认 UTF-8） | |
| 写入方式 | **同步**（`synchronized write`） | 直接 `BufferedOutputStream.write` |
| 旧文件清理 | 超过 totalFileCount 删除最老 | `removeMoreFiles()` |

**文件名样例**：
```
my-app-metrics.log.2026-08-12         ← 当天第 1 个
my-app-metrics.log.2026-08-12.1        ← 当天第 2 个（第 1 个超 50MB 后新建）
my-app-metrics.log.2026-08-12.2        ← 当天第 3 个
my-app-metrics.log.2026-08-12.idx      ← 索引文件
my-app-metrics.log.2026-08-11          ← 昨天
```

**清理逻辑**：`removeMoreFiles` 调用时机为每次新建文件前，会按 `MetricFileNameComparator` 排序后删除多余的（保留最新的 totalFileCount 个）。

### 15.4 `eagleeye-self.log`

| 维度 | 配置 | 说明 |
| --- | --- | --- |
| 单文件大小上限 | 200MB | `MAX_SELF_LOG_FILE_SIZE` |
| 备份份数 | 3 | `EagleEyeRollingFileAppender` 默认 |
| 路径 | `${user.home}/logs/eagleeye/eagleeye-self.log` | 不受 `csp.sentinel.log.dir` 影响 |
| 字符集 | GB18030 | |
| 异常限流 | 10 秒 10 个 | `TokenBucket(10, 10s)` |

### 15.5 切换到 SLF4J 后的策略

如果引入 `sentinel-logging-slf4j` 依赖，RecordLog 与 CommandCenterLog 改由 SLF4J 接管，所有上述 JUL 策略（200MB、4 备份、按日期切分）**全部失效**，改由 Logback / Log4j2 的配置决定。这是生产环境的推荐做法，便于统一日志管理。

但 EagleEye 系列（block、cluster、metric）**不会切换**，仍走 EagleEye 自研框架，因为它们不走 Logger SPI。MetricWriter 也保持独立。

---

## 十六、配置项完整参考

### 16.1 Sentinel 日志相关系统属性（`-D` 参数）

| 配置项 | 默认值 | 说明 | 影响范围 |
| --- | --- | --- | --- |
| `csp.sentinel.log.dir` | `${user.home}/logs/csp/` | 日志根目录 | RecordLog / CommandCenterLog / EagleEye / MetricWriter |
| `csp.sentinel.log.use.pid` | `false` | 文件名是否带 pid | RecordLog / CommandCenterLog / MetricWriter（不含 EagleEye） |
| `csp.sentinel.log.output.type` | `file` | 输出类型（file/console） | RecordLog / CommandCenterLog |
| `csp.sentinel.log.charset` | `utf-8` | 日志字符集 | RecordLog / CommandCenterLog |
| `csp.sentinel.log.level` | `INFO` | 日志级别（ERROR/WARNING/INFO/DEBUG/TRACE） | RecordLog / CommandCenterLog |
| `csp.sentinel.config.file` | `classpath:sentinel.properties` | 配置文件路径 | 所有配置项 |
| `csp.sentinel.charset` | `utf-8` | SentinelConfig 字符集 | MetricWriter 等通过 `SentinelConfig.charset()` 获取 |
| `csp.sentinel.metric.file.single.size` | `52428800`（50MB） | 单个指标文件大小 | MetricWriter |
| `csp.sentinel.metric.file.total.count` | `6` | 指标文件保留份数 | MetricWriter |
| `csp.sentinel.metric.flush.interval` | `1` | 指标写入间隔（秒） | MetricTimerListener |
| `sentinel.rejected.record.period` | `60000` | 拒绝日志打印周期（毫秒） | ConsoleHandler / DateFileLogHandler 的拒绝策略 |

### 16.2 EagleEye 相关系统属性

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `JM.LOG.PATH` | `${user.home}/logs/` | EagleEye 基础日志目录 |
| `EAGLEEYE.LOG.PATH` | `${JM.LOG.PATH}/eagleeye/` | EagleEye 自身日志目录 |
| `EAGLEEYE.CHARSET` | `GB18030`（fallback GBK/UTF-8） | EagleEye 默认字符集 |
| `EAGLEEYE.LOG.SELF.FILESIZE` | `209715200`（200MB） | eagleeye-self.log 大小上限 |
| `project.name` | 无 | 应用名，影响 EagleEye APP_LOG_DIR（实际未用） |

### 16.3 环境变量

| 变量名 | 说明 |
| --- | --- |
| `CSP_SENTINEL_CONFIG_FILE` | 配置文件路径（与 `csp.sentinel.config.file` 等价，但优先级低） |

### 16.4 配置优先级

从高到低：
1. JVM 启动参数 `-D` 系统属性（最高，会覆盖配置文件中的同名项）
2. `csp.sentinel.config.file` 指定的配置文件
3. 环境变量 `CSP_SENTINEL_CONFIG_FILE` 指定的配置文件
4. `classpath:sentinel.properties`
5. 代码中的默认值（最低）

### 16.5 典型 `sentinel.properties` 示例

```properties
# 日志目录
csp.sentinel.log.dir=/var/log/sentinel/
# 文件名带 pid
csp.sentinel.log.use.pid=true
# 输出到文件
csp.sentinel.log.output.type=file
# 字符集
csp.sentinel.log.charset=utf-8
# 日志级别
csp.sentinel.log.level=INFO

# 指标日志
csp.sentinel.metric.file.single.size=52428800
csp.sentinel.metric.file.total.count=6
csp.sentinel.metric.flush.interval=1

# 应用名（也影响指标文件名）
project.name=my-app
csp.sentinel.app.name=my-app
```

---

## 十七、典型调用链路：一次限流请求的日志产生过程

假设应用配置了资源 `com.example.HelloService.sayHello` 的 QPS 限流规则（阈值 10），现在有 20 个并发请求：

### 17.1 完整调用链

```
1. 业务代码调用 SphU.entry("com.example.HelloService.sayHello")
   ↓
2. CtSph.entry() → lookProcessChain → 获取/构建 Slot Chain
   ↓ (首次构建时 RecordLog.info 记录 chain 创建)
3. Slot Chain entry：
   NodeSelectorSlot.entry → fireEntry
   ClusterBuilderSlot.entry → fireEntry
   ↓
4. LogSlot.entry
   try {
     fireEntry →
     StatisticSlot.entry → fireEntry →
     AuthoritySlot.entry → fireEntry →
     SystemSlot.entry → fireEntry →
     FlowSlot.entry
       ├── 检查规则 → 通过的请求 fireEntry（不抛异常）
       └── 超阈值的请求 → throw new FlowException(rule)
     ↓ 异常向上抛
   } catch (BlockException e) {
     ↓ 触发日志
5.   EagleEyeLogUtil.log("com.example.HelloService.sayHello",
                          "FlowException",
                          "default",
                          "originApp",
                          ruleId,
                          1);
     ↓ EagleEye 内部
6.   statLogger.stat(...).count(1)
     ↓ StatEntry 累加到 StatRollingData
7.   （1 秒后）StatLogController roller 任务触发 rolling()
     ↓ CAS 切换 StatRollingData，旧数据交给 writer
8.   writer 线程延迟 200ms 后执行 StatLogWriteTask
     ↓ 遍历所有 StatEntry，格式化为：
     "2026-08-12 14:30:25|sentinel-block-log|com.example.HelloService.sayHello,FlowException,default,originApp,1|10\n"
9.   EagleEyeRollingFileAppender.append(line)
     ↓ SyncAppender synchronized
     ↓ BufferedOutputStream.write(bytes)   ← bytes 是 GB18030 编码
     ↓ 累计 outputByteSize
10.  outputByteSize >= 300MB ?
     ├── 否 → 1 秒后 flush
     └── 是 → rollOver()，重命名 .1/.2/.3
```

### 17.2 产生的日志文件

- **`sentinel-record.log.2026-08-12.0`**：包含 Slot Chain 构建、规则加载等 INFO 日志
- **`sentinel-block.log`**：包含聚合后的限流统计（注意：1 秒内 10 次拒绝只会产生 1 行日志，count=10）
- **`my-app-metrics.log.2026-08-12.0`**：包含该资源的通过 QPS、拒绝 QPS、RT 等（每秒 1 条记录）
- **`command-center.log.2026-08-12.0`**：如果 Dashboard 拉取指标，会有 HTTP 请求日志

### 17.3 典型日志内容

**sentinel-block.log**（GB18030 编码）：
```
2026-08-12 14:30:25|sentinel-block-log|com.example.HelloService.sayHello,FlowException,default,originApp,1|10
2026-08-12 14:30:26|sentinel-block-log|com.example.HelloService.sayHello,FlowException,default,originApp,1|8
```

**my-app-metrics.log.2026-08-12**（UTF-8 编码，每秒 1 条 ClusterNode 数据）：
```
com.example.HelloService.sayHello
0
1691824225000
10
10
0
0
5
0
...

com.example.HelloService.sayHello
0
1691824226000
5
3
0
0
4
0
...
```

**sentinel-record.log.2026-08-12.0**（UTF-8 编码）：
```
2026-08-12 14:30:00.123 INFO Sentinel log output type is: file
2026-08-12 14:30:00.456 INFO Sentinel log base directory is: /var/log/sentinel/
2026-08-12 14:30:01.789 INFO [MetricWriter] Creating new MetricWriter, singleFileSize=52428800, totalFileCount=6
2026-08-12 14:30:02.012 INFO [MetricWriter] New metric file created: /var/log/sentinel/my-app-metrics.log.2026-08-12
2026-08-12 14:30:02.345 INFO [HeartbeatSenderInitFunc] Current heart beat interval: 10000
```

---

## 附录：关键源码文件索引

| 类别 | 文件路径 |
| --- | --- |
| 日志 SPI | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/Logger.java` |
| SPI 加载器 | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/LoggerSpiProvider.java` |
| `@LogTarget` | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/LogTarget.java` |
| `LogBase` | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/LogBase.java` |
| `LogConfigLoader` | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/LogConfigLoader.java` |
| `RecordLog` | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/RecordLog.java` |
| `CommandCenterLog` | `sentinel-transport/sentinel-transport-common/src/main/java/com/alibaba/csp/sentinel/transport/log/CommandCenterLog.java` |
| JUL 适配器 | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/jul/JavaLoggingAdapter.java` |
| JUL 基类 | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/jul/BaseJulLogger.java` |
| 日期文件处理器 | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/jul/DateFileLogHandler.java` |
| 控制台处理器 | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/jul/ConsoleHandler.java` |
| 日志格式 | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/jul/CspFormatter.java` |
| 日志级别 | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/jul/Level.java` |
| SLF4J 占位符 | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/log/jul/MessageFormatter.java` |
| 配置工具 | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/util/ConfigUtil.java` |
| PID 工具 | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/util/PidUtil.java` |
| SLF4J RecordLog 适配 | `sentinel-logging/sentinel-logging-slf4j/src/main/java/com/alibaba/csp/sentinel/logging/slf4j/RecordLogLogger.java` |
| SLF4J CommandCenterLog 适配 | `sentinel-logging/sentinel-logging-slf4j/src/main/java/com/alibaba/csp/sentinel/logging/slf4j/CommandCenterLogLogger.java` |
| EagleEye 入口 | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/eagleeye/EagleEye.java` |
| StatLogger | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/eagleeye/StatLogger.java` |
| StatLoggerBuilder | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/eagleeye/StatLoggerBuilder.java` |
| BaseLoggerBuilder | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/eagleeye/BaseLoggerBuilder.java` |
| StatLogController | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/eagleeye/StatLogController.java` |
| EagleEyeRollingFileAppender | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/eagleeye/EagleEyeRollingFileAppender.java` |
| SyncAppender | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/eagleeye/SyncAppender.java` |
| EagleEyeLogDaemon | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/eagleeye/EagleEyeLogDaemon.java` |
| EagleEyeLogUtil（block） | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/logger/EagleEyeLogUtil.java` |
| LogSlot | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/slots/logger/LogSlot.java` |
| ClusterStatLogUtil | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/cluster/log/ClusterStatLogUtil.java` |
| ClusterClientStatLogUtil | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/cluster/log/ClusterClientStatLogUtil.java` |
| ClusterServerStatLogUtil | `sentinel-cluster/sentinel-cluster-server-default/src/main/java/com/alibaba/csp/sentinel/cluster/server/log/ClusterServerStatLogUtil.java` |
| MetricWriter | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/node/metric/MetricWriter.java` |
| MetricTimerListener | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/node/metric/MetricTimerListener.java` |
| MetricSearcher | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/node/metric/MetricSearcher.java` |
| SentinelConfig | `sentinel-core/src/main/java/com/alibaba/csp/sentinel/config/SentinelConfig.java` |
| FileRefreshableDataSource | `sentinel-extension/sentinel-datasource-extension/src/main/java/com/alibaba/csp/sentinel/datasource/FileRefreshableDataSource.java` |

---

**文档完。**

本文基于 Sentinel 2.0.0-alpha2-SNAPSHOT 分支源码，详细剖析了：
- **5 类日志**：RecordLog、CommandCenterLog、EagleEye 统计（block/cluster/server）、MetricWriter、EagleEye-self
- **2 套底层框架**：JUL（默认）+ EagleEye（自研）
- **1 套 SPI 机制**：Logger SPI，可切换到 SLF4J
- **9 个落盘文件**：sentinel-record / command-center / sentinel-block / sentinel-cluster / sentinel-cluster-client / sentinel-server / metrics.log / metrics.idx / eagleeye-self
- **完整配置项**：12 个系统属性 + 5 个 EagleEye 专属属性 + 1 个环境变量
- **2 种切分策略**：按日期 + 按大小（不同日志组合不同）
- **典型调用链**：从 `SphU.entry()` 到日志落盘的完整过程
