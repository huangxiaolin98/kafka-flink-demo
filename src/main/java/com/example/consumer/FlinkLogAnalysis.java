package com.example.consumer;

import com.example.model.LogEntry;
import com.example.util.LogParser;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import com.example.sink.FileWriterSink;

public class FlinkLogAnalysis {

    // 输出文件路径
    private static final String OUTPUT_PV = "output/pv-stats.log";
    private static final String OUTPUT_ERROR = "output/error-alerts.log";
    private static final String OUTPUT_IP = "output/ip-stats.log";
    private static final String OUTPUT_LATENCY = "output/latency-stats.log";

    public static void main(String[] args) throws Exception {
        // 1. 创建 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 不硬编码并行度，由运行环境决定：
        // - 集群模式(flink run)：使用 flink-conf.yaml 中的 parallelism.default
        // - 本地模式(java -cp)：默认使用 CPU 核心数
        // 启用对象复用，避免算子间深拷贝，减少 GC 压力
        env.getConfig().enableObjectReuse();
        // 设置缓冲超时为 5ms，降低算子间数据交换延迟
        env.setBufferTimeout(5);

        // 2. 配置 Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("access-log")
            .setGroupId("flink-log-consumer-group")
            .setStartingOffsets(OffsetsInitializer.latest()) // 从最新位置开始消费，忽略历史数据
            .setValueOnlyDeserializer(new SimpleStringSchema())
            // 消费端拉取优化：增大每次拉取的数据量，减少网络请求次数
            .setProperty("fetch.min.bytes", "1048576")          // 每次至少拉取 1MB
            .setProperty("fetch.max.wait.ms", "100")            // 最多等待 100ms 凑够数据
            .setProperty("max.partition.fetch.bytes", "2097152") // 每分区每次最多拉取 2MB
            .build();

        // 3. 读取 Kafka 数据流
        DataStream<String> rawStream = env.fromSource(
            kafkaSource,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );

        // 4. 解析日志字符串为 LogEntry 对象
        DataStream<LogEntry> logStream = rawStream.map(new MapFunction<String, LogEntry>() {
            @Override
            public LogEntry map(String line) throws Exception {
                return LogParser.parse(line);
            }
        }).filter(log -> log != null); // 过滤掉解析失败的 null 值

        // ========== 功能一：PV统计（5秒滚动窗口）==========
        // 采用显式的 MapReduce 模式：Map阶段 -> Shuffle阶段 -> Reduce阶段
        DataStream<String> pvStream = logStream
            // --- Map 阶段：将每条日志映射为 <apiPath, 1> 键值对 ---
            .map(log -> Tuple2.of(log.getApiPath(), 1))
            .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                org.apache.flink.api.common.typeinfo.Types.STRING,
                org.apache.flink.api.common.typeinfo.Types.INT))
            // --- Shuffle 阶段：按 API 路径分区，相同 key 的数据汇聚到一起 ---
            .keyBy(t -> t.f0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            // --- Reduce 阶段：对同一 key 的计数值进行累加聚合 ---
            .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> reduce(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                    return Tuple2.of(a.f0, a.f1 + b.f1);
                }
            })
            .map(t -> "[PV统计] 接口: " + t.f0 + " | 5秒内访问次数: " + t.f1)
            .returns(org.apache.flink.api.common.typeinfo.Types.STRING);

        pvStream.print();
        pvStream.addSink(new FileWriterSink(OUTPUT_PV))
                .setParallelism(1)
                .name("PV File Sink");

        // ========== 功能二：ERROR异常检测 ==========
        // 实时捕获 ERROR 级别日志并告警
        DataStream<String> errorStream = logStream
            .filter(log -> "ERROR".equals(log.getLevel())) // 过滤出 ERROR 日志
            .map(log -> "[ERROR告警] 检测到异常！路径: " + log.getApiPath()
                + " | IP: " + log.getIp()
                + " | 响应时间: " + log.getResponseTime() + "ms")
            .returns(org.apache.flink.api.common.typeinfo.Types.STRING);

        errorStream.print();
        errorStream.addSink(new FileWriterSink(OUTPUT_ERROR))
                   .setParallelism(1)
                   .name("Error Alert File Sink");

        // ========== 功能三：IP频率分析（10秒滑动窗口，每5秒滑动一次）==========
        // 同样采用 MapReduce 模式，识别高频访问 IP
        DataStream<String> ipStream = logStream
            // --- Map 阶段：将日志映射为 <IP, 1> 键值对 ---
            .map(log -> Tuple2.of(log.getIp(), 1))
            .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                org.apache.flink.api.common.typeinfo.Types.STRING,
                org.apache.flink.api.common.typeinfo.Types.INT))
            // --- Shuffle 阶段：按 IP 分区 ---
            .keyBy(t -> t.f0)
            .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
            // --- Reduce 阶段：累加同一 IP 的访问计数 ---
            .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> reduce(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                    return Tuple2.of(a.f0, a.f1 + b.f1);
                }
            })
            .filter(t -> t.f1 > 5)  // 阈值：10秒内访问超过5次则标记
            .map(t -> "[IP分析] 时间: " + java.time.LocalTime.now().format(
                java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"))
                + " | 高频访问 IP: " + t.f0
                + " | 10秒内访问次数: " + t.f1)
            .returns(org.apache.flink.api.common.typeinfo.Types.STRING);

        ipStream.print();
        ipStream.addSink(new FileWriterSink(OUTPUT_IP))
                .setParallelism(1)
                .name("IP Analysis File Sink");

        // ========== 功能四：延迟监控（10秒滚动窗口，用于压力测试）==========
        // 计算每条消息从 Producer 生成到 Flink 处理的端到端延迟
        // 使用全局窗口统计平均延迟、最大延迟和背压状态
        DataStream<String> latencyStream = logStream
            // --- Map 阶段：计算每条消息的处理延迟 ---
            .map(log -> {
                long genTime = log.getTimestampMillis();
                long now = System.currentTimeMillis();
                long latency = genTime > 0 ? Math.max(0, now - genTime) : 0;
                return Tuple3.of(latency, latency, 1L);
            })
            .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                org.apache.flink.api.common.typeinfo.Types.LONG,
                org.apache.flink.api.common.typeinfo.Types.LONG,
                org.apache.flink.api.common.typeinfo.Types.LONG))
            // --- 全局 10 秒滚动窗口聚合 ---
            .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
            // --- Reduce 阶段：累加延迟总和、取最大延迟、累加计数 ---
            .reduce(new ReduceFunction<Tuple3<Long, Long, Long>>() {
                @Override
                public Tuple3<Long, Long, Long> reduce(Tuple3<Long, Long, Long> a, Tuple3<Long, Long, Long> b) {
                    return Tuple3.of(a.f0 + b.f0, Math.max(a.f1, b.f1), a.f2 + b.f2);
                }
            })
            // --- 格式化输出：平均延迟、最大延迟、消息量、背压判断 ---
            .map(t -> {
                long avgLatency = t.f2 > 0 ? t.f0 / t.f2 : 0;
                long maxLatency = t.f1;
                long msgCount = t.f2;
                String backpressure;
                if (avgLatency < 100) {
                    backpressure = "无背压";
                } else if (avgLatency < 500) {
                    backpressure = "轻微背压";
                } else {
                    backpressure = "明显背压";
                }
                return "[延迟监控] 窗口消息数: " + msgCount
                    + " | 平均延迟: " + avgLatency + "ms"
                    + " | 最大延迟: " + maxLatency + "ms"
                    + " | 背压状态: " + backpressure;
            })
            .returns(org.apache.flink.api.common.typeinfo.Types.STRING);

        latencyStream.print();
        latencyStream.addSink(new FileWriterSink(OUTPUT_LATENCY))
                     .setParallelism(1)
                     .name("Latency Monitor File Sink");

        // 5. 启动 Flink 任务
        
        env.execute("Kafka-Flink Real-time Log Analysis");
    }
}
