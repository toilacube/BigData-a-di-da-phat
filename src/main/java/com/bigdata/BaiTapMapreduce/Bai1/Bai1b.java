package com.bigdata.BaiTapMapreduce.Bai1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai1b {
    public static class MMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text url = new Text();
        private IntWritable timeSpent = new IntWritable();
        private Map<String, Integer> urlTimeMap = new HashMap<>(); // Lưu tổng cục bộ URL và thời gian
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");

            if (fields.length == 2) {
                String urlStr = fields[0].trim();
                Integer timeSpentValue = Integer.parseInt(fields[1].trim());
                urlTimeMap.put(urlStr, urlTimeMap.getOrDefault(urlStr, 0) + timeSpentValue);
                context.write(url, timeSpent);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Ghi toàn bộ dữ liệu cục bộ ra context sau khi map hoàn tất
            for (Map.Entry<String, Integer> entry : urlTimeMap.entrySet()) {
                url.set(entry.getKey());
                timeSpent.set(entry.getValue());
                context.write(url, timeSpent);
            }
        }
    }

    public static class RReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Web count");
        job.setJarByClass(Bai1b.class);
        job.setMapperClass(MMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(RReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
