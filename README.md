# Kafka-Flink 实时日志分析系统 -- 云计算作业

基于 Apache Kafka + Apache Flink + Hadoop MapReduce 的日志处理 Demo，模拟 Web 服务访问日志的采集、传输与分析。同时包含**实时流处理**和**批处理**两种模式，体现 MapReduce 编程范式。

## 项目架构

```
=== 实时处理链路（Kafka + Flink）===

LogProducer (Kafka Producer)
    │  模拟生成访问日志，发送到 Kafka
    ▼
Kafka Topic: access-log
    │
    ▼
FlinkLogAnalysis (Flink Consumer，内部采用 MapReduce 模式)
    ├── PV 统计（5 秒滚动窗口）── Map → Shuffle → Reduce
    ├── ERROR 异常实时告警
    └── IP 高频访问检测（10 秒滑动窗口）── Map → Shuffle → Reduce

=== 批处理链路（Hadoop MapReduce）===

LogFileGenerator
    │  生成模拟日志文件
    ▼
logs/access.log
    │
    ├──► LogPvMapReduce  ──► output/pv-result/   (API 路径 PV 统计)
    └──► LogIpMapReduce  ──► output/ip-result/   (IP 访问量统计)
```

## 功能说明

### 实时处理（Flink 流式 MapReduce）

| 功能 | 窗口类型 | MapReduce 模式 |
|------|---------|---------------|
| PV 统计 | 5 秒滚动窗口 (Tumbling) | Map(log→<path,1>) → keyBy → Reduce(sum) |
| ERROR 告警 | 无窗口（实时） | Filter + Map |
| IP 频率分析 | 10 秒滑动窗口 (Sliding, 步长 5s) | Map(log→<ip,1>) → keyBy → Reduce(sum) |

### 批处理（Hadoop MapReduce）

| Job | Map 输出 | Reduce 输出 | 说明 |
|-----|---------|------------|------|
| LogPvMapReduce | <API路径, 1> | <API路径, 总PV> | 统计每个接口的总访问量 |
| LogIpMapReduce | <IP, 1> | <IP, 总访问次数> | 统计每个 IP 的总访问量 |

## 模块结构

```
src/main/java/com/example/
├── model/
│   └── LogEntry.java              # 日志数据模型
├── producer/
│   └── LogProducer.java           # Kafka 生产者，模拟日志数据
├── consumer/
│   └── FlinkLogAnalysis.java      # Flink 消费端，流式 MapReduce 分析
├── mapreduce/
│   ├── LogPvMapReduce.java        # Hadoop MapReduce：API PV 统计
│   └── LogIpMapReduce.java        # Hadoop MapReduce：IP 访问量统计
└── util/
    ├── LogParser.java             # 日志字符串解析工具
    └── LogFileGenerator.java      # 日志文件生成器（供 MapReduce 使用）
```

## 技术栈

- **Java** 11
- **Apache Flink** 1.17.0（实时流处理）
- **Apache Kafka** 2.8.0（消息队列）
- **Hadoop MapReduce** 3.3.4（批处理）
- **Maven**（使用 Shade 插件打包 uber-jar）

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

### 实时处理（Kafka + Flink）

**1. 启动 Kafka Producer（模拟日志）**

```bash
java -cp target/kafka-flink-demo-1.0-SNAPSHOT.jar com.example.producer.LogProducer
```

Producer 将以每秒约 10 条的速率持续发送模拟日志。

**2. 启动 Flink 实时分析**

```bash
java -cp target/kafka-flink-demo-1.0-SNAPSHOT.jar com.example.consumer.FlinkLogAnalysis
```

### 批处理（Hadoop MapReduce）

**1. 生成模拟日志文件**

```bash
java -cp target/kafka-flink-demo-1.0-SNAPSHOT.jar com.example.util.LogFileGenerator logs/access.log 5000
```

参数说明：第一个参数为输出文件路径，第二个参数为生成的日志条数（默认 1000）。

**2. 运行 PV 统计 MapReduce Job**

```bash
java -cp target/kafka-flink-demo-1.0-SNAPSHOT.jar com.example.mapreduce.LogPvMapReduce logs/access.log output/pv-result
```

**3. 运行 IP 统计 MapReduce Job**

```bash
java -cp target/kafka-flink-demo-1.0-SNAPSHOT.jar com.example.mapreduce.LogIpMapReduce logs/access.log output/ip-result
```

结果文件位于 `output/pv-result/part-r-00000` 和 `output/ip-result/part-r-00000`。

## 日志格式

```
2026-04-13 17:30:00 | INFO  | /api/login      | 192.168.1.1     | 200 | 45ms
2026-04-13 17:30:01 | ERROR | /api/payment    | 10.0.0.1        | 500 | 2103ms
```

字段依次为：时间戳、日志级别、API 路径、客户端 IP、HTTP 状态码、响应时间。

## 输出示例

### Flink 实时输出

```
[PV统计] 接口: /api/login | 5秒内访问次数: 8
[ERROR告警] 检测到异常！路径: /api/payment | IP: 10.0.0.1 | 响应时间: 2103ms
[IP分析] 高频访问 IP: 192.168.1.1 | 10秒内访问次数: 7
```

### MapReduce 批处理输出

**PV 统计结果 (output/pv-result/part-r-00000)：**
```
/api/login      856
/api/logout     812
/api/order      843
/api/payment    830
/api/product    825
/api/user       834
```

**IP 统计结果 (output/ip-result/part-r-00000)：**
```
10.0.0.1        832
10.0.0.2        841
172.16.0.1      826
192.168.1.1     838
192.168.1.2     830
192.168.1.3     833
```
