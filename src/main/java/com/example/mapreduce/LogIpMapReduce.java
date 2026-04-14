package com.example.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Hadoop MapReduce 批处理 Job：按 IP 统计访问量
 *
 * 输入：日志文件（每行格式：时间戳 | 级别 | 路径 | IP | 状态码 | 响应时间）
 * 输出：每个 IP 的总访问次数，按访问量降序排列
 *
 * MapReduce 流程：
 *   Map 阶段：解析每行日志，提取客户端 IP，输出 <IP, 1>
 *   Shuffle 阶段（框架自动完成）：按 IP 分组
 *   Reduce 阶段：对同一 IP 的计数求和，输出 <IP, totalCount>
 */
public class LogIpMapReduce {

    /**
     * Mapper：将每条日志映射为 <IP, 1> 键值对
     * 输入：<行偏移量, 日志文本行>
     * 输出：<IP, 1>
     */
    public static class IpMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text ip = new Text();
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            // 解析日志行：时间戳 | 级别 | 路径 | IP | 状态码 | 响应时间
            String[] parts = line.split("\\|");
            if (parts.length >= 6) {
                String ipAddr = parts[3].trim();
                ip.set(ipAddr);
                context.write(ip, one);
            }
        }
    }

    /**
     * Reducer：对同一 IP 的访问计数进行累加
     * 输入：<IP, [1, 1, 1, ...]>
     * 输出：<IP, 总访问次数>
     */
    public static class IpReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("用法: LogIpMapReduce <输入路径> <输出路径>");
            System.err.println("示例: LogIpMapReduce ./logs/access.log ./output/ip-result");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Log IP Statistics (MapReduce)");

        job.setJarByClass(LogIpMapReduce.class);
        job.setMapperClass(IpMapper.class);
        job.setCombinerClass(IpReducer.class); // Combiner 预聚合
        job.setReducerClass(IpReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.out.println("========================================");
        System.out.println("  MapReduce Job: IP 访问量统计");
        System.out.println("  输入路径: " + args[0]);
        System.out.println("  输出路径: " + args[1]);
        System.out.println("========================================");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
