package com.example.model;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogEntry implements Serializable {
    private static final long serialVersionUID = 1L;

    private String timestamp;
    private String level;
    private String apiPath;
    private String ip;
    private int statusCode;
    private long responseTime;

    public LogEntry() {}

    public LogEntry(String timestamp, String level, String apiPath,
                    String ip, int statusCode, long responseTime) {
        this.timestamp = timestamp;
        this.level = level;
        this.apiPath = apiPath;
        this.ip = ip;
        this.statusCode = statusCode;
        this.responseTime = responseTime;
    }

    // Getters
    public String getTimestamp() { return timestamp; }
    public String getLevel() { return level; }
    public String getApiPath() { return apiPath; }
    public String getIp() { return ip; }
    public int getStatusCode() { return statusCode; }
    public long getResponseTime() { return responseTime; }

    /**
     * 将时间戳字符串转换为 epoch 毫秒值，用于计算处理延迟。
     * 兼容 "yyyy-MM-dd HH:mm:ss.SSS" 和 "yyyy-MM-dd HH:mm:ss" 两种格式。
     */
    public long getTimestampMillis() {
        if (timestamp == null) return 0L;
        try {
            SimpleDateFormat sdfMs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            Date date = sdfMs.parse(timestamp);
            return date.getTime();
        } catch (Exception e1) {
            try {
                SimpleDateFormat sdfSec = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date date = sdfSec.parse(timestamp);
                return date.getTime();
            } catch (Exception e2) {
                return 0L;
            }
        }
    }

    @Override
    public String toString() {
        return timestamp + " | " + level + " | " + apiPath +
               " | " + ip + " | " + statusCode + " | " + responseTime + "ms";
    }
}
