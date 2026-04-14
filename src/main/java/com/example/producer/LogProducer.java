package com.example.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class LogProducer {
    private static final String TOPIC = "access-log";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092"; // 都在本机，用localhost

    private static final String[] API_PATHS = {
        "/api/login", "/api/logout", "/api/product",
        "/api/payment", "/api/user", "/api/order"
    };

    private static final String[] IP_LIST = {
        "192.168.1.1", "192.168.1.2", "192.168.1.3",
        "10.0.0.1", "10.0.0.2", "172.16.0.1"
    };

    private static final String[] LOG_LEVELS = {
        "INFO","INFO","INFO","INFO","INFO",
        "INFO","INFO","INFO","WARN","ERROR"
    };

    public static void main(String[] args) throws InterruptedException {
        // 解析命令行参数：发送间隔(ms) 和 测试持续时间(s)
        // 无参数 = 正常模式（100ms间隔，无限运行）
        // 有参数 = 压力测试模式（自定义间隔，限时运行）
        boolean stressTestMode = args.length >= 1;

        int interval = 100;  // 默认 100ms
        int duration = 30;   // 默认 30 秒（仅压测模式生效）

        if (args.length >= 1) {
            try {
                interval = Integer.parseInt(args[0]);
                if (interval < 0) interval = 0;
            } catch (NumberFormatException e) {
                System.err.println("无效的发送间隔参数，使用默认值 100ms");
            }
        }
        if (args.length >= 2) {
            try {
                duration = Integer.parseInt(args[1]);
                if (duration <= 0) duration = 30;
            } catch (NumberFormatException e) {
                System.err.println("无效的持续时间参数，使用默认值 30s");
            }
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        if (stressTestMode) {
            // 压测模式：acks=1 降低发送延迟，增大批量参数提升吞吐
            props.put(ProducerConfig.ACKS_CONFIG, "1");
            props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);
        } else {
            props.put(ProducerConfig.ACKS_CONFIG, "all");
        }
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Random random = new Random();
        // 升级为毫秒精度时间戳，供 Flink 端计算处理延迟
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        if (stressTestMode) {
            System.out.println("========================================");
            System.out.println(">>> 压力测试模式");
            System.out.println(">>> 发送间隔: " + (interval == 0 ? "不限速(0ms)" : interval + "ms"));
            System.out.println(">>> 持续时间: " + duration + "s");
            System.out.println(">>> Topic: " + TOPIC);
            System.out.println("========================================");
        } else {
            System.out.println(">>> 正常模式启动，发送间隔: 100ms，Topic: " + TOPIC);
        }

        // 统计变量
        long totalSent = 0;
        long testStartTime = System.currentTimeMillis();
        long lastReportTime = testStartTime;
        long lastReportCount = 0;
        AtomicLong callbackLatencySum = new AtomicLong(0);
        AtomicLong callbackCount = new AtomicLong(0);

        long testEndTime = stressTestMode ? testStartTime + (long) duration * 1000 : Long.MAX_VALUE;

        while (System.currentTimeMillis() < testEndTime) {
            String timestamp = sdf.format(new Date());
            String level = LOG_LEVELS[random.nextInt(LOG_LEVELS.length)];
            String apiPath = API_PATHS[random.nextInt(API_PATHS.length)];
            String ip = IP_LIST[random.nextInt(IP_LIST.length)];

            int statusCode = level.equals("ERROR") ? 500 : 200;
            long responseTime = level.equals("ERROR") ?
                               1000 + random.nextInt(2000) :
                               10 + random.nextInt(200);

            String logMessage = String.format("%s | %-5s | %-15s | %-15s | %d | %dms",
                timestamp, level, apiPath, ip, statusCode, responseTime);

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, ip, logMessage);

            long sendStart = System.currentTimeMillis();
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Send Failed: " + exception.getMessage());
                } else {
                    long latency = System.currentTimeMillis() - sendStart;
                    callbackLatencySum.addAndGet(latency);
                    callbackCount.incrementAndGet();
                }
            });

            totalSent++;

            // 压测模式：每秒输出一次统计信息
            if (stressTestMode) {
                long now = System.currentTimeMillis();
                if (now - lastReportTime >= 1000) {
                    long elapsedSec = (now - testStartTime) / 1000;
                    long thisSec = totalSent - lastReportCount;
                    long cbCount = callbackCount.get();
                    long avgCbLatency = cbCount > 0 ? callbackLatencySum.get() / cbCount : 0;
                    System.out.printf("[统计] 已运行 %ds | 本秒发送: %d msg | 累计: %d msg | 平均回调延迟: %dms%n",
                        elapsedSec, thisSec, totalSent, avgCbLatency);
                    lastReportTime = now;
                    lastReportCount = totalSent;
                }
            } else {
                // 正常模式：每 100 条打印一次
                if (totalSent % 100 == 0) {
                    System.out.println("已发送 " + totalSent + " 条日志");
                }
            }

            if (interval > 0) {
                Thread.sleep(interval);
            }
        }

        // 确保所有消息都已发送
        producer.flush();

        // 压测模式：打印测试总结
        if (stressTestMode) {
            long totalTime = System.currentTimeMillis() - testStartTime;
            double totalTimeSec = totalTime / 1000.0;
            double avgThroughput = totalSent / totalTimeSec;
            long cbCount = callbackCount.get();
            long avgCbLatency = cbCount > 0 ? callbackLatencySum.get() / cbCount : 0;

            System.out.println();
            System.out.println("========== 测试总结 ==========");
            System.out.println("发送间隔: " + (interval == 0 ? "不限速(0ms)" : interval + "ms"));
            System.out.println("实际持续时间: " + String.format("%.1f", totalTimeSec) + "s");
            System.out.printf("总发送消息数: %d%n", totalSent);
            System.out.printf("平均吞吐量: %.1f msg/sec%n", avgThroughput);
            System.out.printf("平均回调延迟: %dms%n", avgCbLatency);
            System.out.println("==============================");
        }

        producer.close();
    }
}
