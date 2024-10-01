package com.bigdata.Bai6;

import com.bigdata.Bai1.Bai1a;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Bai6 {
    public static class MMapper extends Mapper<Object, Text, Text, IntWritable> {
            private final IntWritable valueWritable = new IntWritable();
            private final Text url = new Text();

            @Override
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                String[] parts = value.toString().trim().split("\\s+");
                if (parts.length == 2) {
                    url.set(parts[0]);
                    valueWritable.set(Integer.parseInt(parts[1]));
                    context.write(url, valueWritable);
                }
            }
        }

        public static class RReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
            private Text minUrl = new Text();
            private IntWritable minValue = new IntWritable(Integer.MAX_VALUE);
            private Text maxUrl = new Text();
            private IntWritable maxValue = new IntWritable(Integer.MIN_VALUE);

            public void reduce(Text key, Iterable<IntWritable> values, Context context)
                    throws IOException, InterruptedException {
                for (IntWritable value : values) {
                    if (value.get() < minValue.get()) {
                        minValue.set(value.get());
                        minUrl.set(key);
                    }
                    if (value.get() > maxValue.get()) {
                        maxValue.set(value.get());
                        maxUrl.set(key);
                    }
                }
            }

            protected void cleanup(Context context) throws IOException, InterruptedException {
                context.write(minUrl, minValue);
                context.write(maxUrl, maxValue);
            }
        }

        public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MapReduce");
        job.setJarByClass(Bai1a.class);
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
