package com.bigdata.Bai1;

import java.io.IOException;

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

public class Bai1a {
    public static class Bai1Mapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text url = new Text();
        private IntWritable timeSpent = new IntWritable();

        // Change IntWritable key to Object (or LongWritable)
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Split the line by commas (assuming it's a CSV file)
            String[] fields = value.toString().split(",");

            if (fields.length == 2) {
                String urlStr = fields[0].trim(); // URL
                Integer timeSpentValue;

                try {
                    timeSpentValue = Integer.parseInt(fields[1].trim()); // Time spent in minutes
                } catch (NumberFormatException e) {
                    // If the time value is not a valid number, skip this record
                    return;
                }

                // Set the values for output key-value pairs
                url.set(urlStr);
                timeSpent.set(timeSpentValue);

                // Emit the key-value pair: (url, time_spent)
                context.write(url, timeSpent);
            }
        }
    }

    public static class Bai1Reducer
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
        job.setJarByClass(Bai1a.class);
        job.setMapperClass(Bai1Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(Bai1Reducer.class);
        job.setReducerClass(Bai1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
