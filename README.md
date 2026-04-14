# Kafka-Flink 实时日志分析系统 -- 云计算作业

基于 Apache Kafka + Apache Flink 的实时日志流处理 Demo，模拟 Web 服务访问日志的采集、传输与实时分析。

## 项目架构

```
LogProducer (Kafka Producer)
    │  模拟生成访问日志，发送到 Kafka
    ▼
Kafka Topic: access-log
    │
    ▼
FlinkLogAnalysis (Flink Consumer)
    ├── PV 统计（5 秒滚动窗口）
    ├── ERROR 异常实时告警
    └── IP 高频访问检测（10 秒滑动窗口）
```

## 功能说明

| 功能 | 窗口类型 | 说明 |
|------|---------|------|
| PV 统计 | 5 秒滚动窗口 (Tumbling) | 统计每个 API 接口在窗口内的访问次数 |
| ERROR 告警 | 无窗口（实时） | 捕获 ERROR 级别日志并输出告警信息 |
| IP 频率分析 | 10 秒滑动窗口 (Sliding, 步长 5s) | 识别 10 秒内访问超过 5 次的高频 IP |

## 模块结构

```
src/main/java/com/example/
├── model/
│   └── LogEntry.java          # 日志数据模型
├── producer/
│   └── LogProducer.java       # Kafka 生产者，模拟日志数据
├── consumer/
│   └── FlinkLogAnalysis.java  # Flink 消费端，实时分析逻辑
└── util/
    └── LogParser.java         # 日志字符串解析工具
```

## 技术栈

- **Java** 11
- **Apache Flink** 1.17.0
- **Apache Kafka** 2.8.0
- **Maven** (使用 Shade 插件打包 uber-jar)

## 环境准备

1. 安装并启动 **Zookeeper** 和 **Kafka**（默认监听 `localhost:9092`）
2. 创建 Kafka Topic：

```bash
kafka-topics.sh --create --topic access-log --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## 构建

```bash
mvn clean package
```

构建产物位于 `target/kafka-flink-demo-1.0-SNAPSHOT.jar`（包含所有依赖的 uber-jar）。

## 运行

**1. 启动 Kafka Producer（模拟日志）**

```bash
java -cp target/kafka-flink-demo-1.0-SNAPSHOT.jar com.example.producer.LogProducer
```

Producer 将以每秒约 10 条的速率持续发送模拟日志。

**2. 启动 Flink 实时分析**

```bash
java -cp target/kafka-flink-demo-1.0-SNAPSHOT.jar com.example.consumer.FlinkLogAnalysis
```

## 日志格式

```
2026-04-13 17:30:00 | INFO  | /api/login      | 192.168.1.1     | 200 | 45ms
2026-04-13 17:30:01 | ERROR | /api/payment    | 10.0.0.1        | 500 | 2103ms
```

字段依次为：时间戳、日志级别、API 路径、客户端 IP、HTTP 状态码、响应时间。

## 输出示例

```
[PV统计] 接口: /api/login | 5秒内访问次数: 8
[ERROR告警] 检测到异常！路径: /api/payment | IP: 10.0.0.1 | 响应时间: 2103ms
[IP分析] 高频访问 IP: 192.168.1.1 | 10秒内访问次数: 7
```
