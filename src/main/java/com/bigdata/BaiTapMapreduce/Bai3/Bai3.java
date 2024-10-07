package com.bigdata.BaiTapMapreduce.Bai3;

import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai3 {
    public static class MMapper extends Mapper<Object, Text, Text, PairWritable> {
        private PairWritable pair = new PairWritable();
        public void map(Object key, Text data, Context context) throws IOException, InterruptedException {
            String fields[] = data.toString().split(",");
            if (fields.length == 3) {

                String air = fields[1];
                if (!air.equals("CO"))
                    return;
                double temp = Double.parseDouble(fields[2]);
                pair.setFirst(new DoubleWritable(temp));
                pair.setSecond(new IntWritable(1));

                context.write(new Text(air), pair);
            }
        }

    }

    public static class RReducer
            extends Reducer<Text, PairWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<PairWritable> values,
                           Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (PairWritable val : values) {
                sum += val.getFirst().get();
                count += val.getSecond().get();
            }
            result.set(sum/count);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Bai3.class);
        job.setMapperClass(MMapper.class);
        job.setMapOutputValueClass(PairWritable.class);
//        job.setCombinerClass(RReducer.class);
        job.setReducerClass(RReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
