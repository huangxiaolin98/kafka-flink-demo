package com.example.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.text.SimpleDateFormat;
import java.util.*;

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
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Random random = new Random();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        System.out.println(">>> Kafka Producer Started. Sending logs to " + TOPIC);

        int count = 0;
        while (true) {
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

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Send Failed: " + exception.getMessage());
                }
            });

            count++;
            if (count % 100 == 0) {
                System.out.println("Sent " + count + " records. Latest: " + logMessage);
            }

            // 模拟每秒10条数据 (100ms间隔)
            Thread.sleep(100);
        }
    }
}
