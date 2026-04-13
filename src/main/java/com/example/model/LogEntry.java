package com.example.model;

import java.io.Serializable;

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

    @Override
    public String toString() {
        return timestamp + " | " + level + " | " + apiPath +
               " | " + ip + " | " + statusCode + " | " + responseTime + "ms";
    }
}
