package com.example.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 自定义文件写入 Sink：将流数据追加写入指定文件
 *
 * 每条记录会附加时间戳前缀，格式：[yyyy-MM-dd HH:mm:ss] 原始内容
 * 使用追加模式，重启不会覆盖已有数据。
 * 建议配合 .setParallelism(1) 使用，避免多实例写同一文件的竞态。
 */
public class FileWriterSink extends RichSinkFunction<String> implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(FileWriterSink.class);

    private final String filePath;

    private transient BufferedWriter writer;
    private transient SimpleDateFormat dateFormat;

    public FileWriterSink(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        File file = new File(filePath);
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }

        // 追加模式打开文件
        writer = new BufferedWriter(new FileWriter(file, true));
        dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        LOG.info("FileWriterSink opened: {}", file.getAbsolutePath());
    }

    @Override
    public void invoke(String value, Context context) {
        try {
            String timestamp = dateFormat.format(new Date());
            writer.write("[" + timestamp + "] " + value);
            writer.newLine();
            writer.flush();
        } catch (Exception e) {
            LOG.warn("Failed to write to file {}: {}", filePath, e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            try {
                writer.close();
            } catch (Exception e) {
                LOG.warn("Failed to close writer for {}: {}", filePath, e.getMessage());
            }
        }
        super.close();
    }
}
