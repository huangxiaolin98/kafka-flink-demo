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
 * Hadoop MapReduce 批处理 Job：按 API 路径统计 PV（页面访问量）
 *
 * 输入：日志文件（每行格式：时间戳 | 级别 | 路径 | IP | 状态码 | 响应时间）
 * 输出：每个 API 路径的总访问次数
 *
 * MapReduce 流程：
 *   Map 阶段：解析每行日志，提取 API 路径，输出 <apiPath, 1>
 *   Shuffle 阶段（框架自动完成）：按 apiPath 分组
 *   Reduce 阶段：对同一 apiPath 的计数求和，输出 <apiPath, totalCount>
 */
public class LogPvMapReduce {

    /**
     * Mapper：将每条日志映射为 <API路径, 1> 键值对
     * 输入：<行偏移量, 日志文本行>
     * 输出：<API路径, 1>
     */
    public static class PvMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text apiPath = new Text();
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
                String path = parts[2].trim();
                apiPath.set(path);
                context.write(apiPath, one);
            }
        }
    }

    /**
     * Reducer：对同一 API 路径的访问计数进行累加
     * 输入：<API路径, [1, 1, 1, ...]>
     * 输出：<API路径, 总访问次数>
     */
    public static class PvReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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
            System.err.println("用法: LogPvMapReduce <输入路径> <输出路径>");
            System.err.println("示例: LogPvMapReduce ./logs/access.log ./output/pv-result");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Log PV Statistics (MapReduce)");

        job.setJarByClass(LogPvMapReduce.class);
        job.setMapperClass(PvMapper.class);
        job.setCombinerClass(PvReducer.class); // 使用 Combiner 在 Map 端预聚合，减少 Shuffle 数据量
        job.setReducerClass(PvReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.out.println("========================================");
        System.out.println("  MapReduce Job: API PV 统计");
        System.out.println("  输入路径: " + args[0]);
        System.out.println("  输出路径: " + args[1]);
        System.out.println("========================================");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
