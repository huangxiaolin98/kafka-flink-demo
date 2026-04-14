package com.example.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * 日志文件生成器：生成模拟日志文件供 Hadoop MapReduce 批处理使用
 *
 * 生成的日志格式与 Kafka Producer 一致：
 *   时间戳 | 级别 | 路径 | IP | 状态码 | 响应时间
 */
public class LogFileGenerator {

    private static final String[] API_PATHS = {
        "/api/login", "/api/logout", "/api/product",
        "/api/payment", "/api/user", "/api/order"
    };

    private static final String[] IP_LIST = {
        "192.168.1.1", "192.168.1.2", "192.168.1.3",
        "10.0.0.1", "10.0.0.2", "172.16.0.1"
    };

    private static final String[] LOG_LEVELS = {
        "INFO", "INFO", "INFO", "INFO", "INFO",
        "INFO", "INFO", "INFO", "WARN", "ERROR"
    };

    public static void main(String[] args) throws IOException {
        int count = 1000; // 默认生成 1000 条
        String outputPath = "logs/access.log";

        if (args.length >= 1) {
            outputPath = args[0];
        }
        if (args.length >= 2) {
            count = Integer.parseInt(args[1]);
        }

        File outputFile = new File(outputPath);
        outputFile.getParentFile().mkdirs();

        Random random = new Random();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            for (int i = 0; i < count; i++) {
                String timestamp = sdf.format(new Date(System.currentTimeMillis() - random.nextInt(3600000)));
                String level = LOG_LEVELS[random.nextInt(LOG_LEVELS.length)];
                String apiPath = API_PATHS[random.nextInt(API_PATHS.length)];
                String ip = IP_LIST[random.nextInt(IP_LIST.length)];

                int statusCode = level.equals("ERROR") ? 500 : 200;
                long responseTime = level.equals("ERROR") ?
                    1000 + random.nextInt(2000) :
                    10 + random.nextInt(200);

                String logLine = String.format("%s | %-5s | %-15s | %-15s | %d | %dms",
                    timestamp, level, apiPath, ip, statusCode, responseTime);

                writer.write(logLine);
                writer.newLine();
            }
        }

        System.out.println("日志文件已生成: " + outputFile.getAbsolutePath());
        System.out.println("共生成 " + count + " 条日志记录");
    }
}
