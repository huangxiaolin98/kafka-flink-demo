package com.example.util;

import com.example.model.LogEntry;

public class LogParser {

    /**
     * 将原始日志字符串解析为 LogEntry 对象
     * 格式：时间戳 | 级别 | 路径 | IP | 状态码 | 响应时间
     */
    public static LogEntry parse(String line) {
        try {
            // 注意：split 中的 "|" 是正则特殊字符，需要转义为 "\\|"
            String[] parts = line.split("\\|");
            
            // 简单的校验，防止数组越界
            if (parts.length < 6) {
                return null;
            }

            return new LogEntry(
                parts[0].trim(),
                parts[1].trim(),
                parts[2].trim(),
                parts[3].trim(),
                Integer.parseInt(parts[4].trim()),
                Long.parseLong(parts[5].trim().replace("ms", ""))
            );
        } catch (Exception e) {
            // 解析失败返回 null，后续在 Flink 中过滤掉
            System.err.println("Parse Error: " + line);
            return null;
        }
    }
}
