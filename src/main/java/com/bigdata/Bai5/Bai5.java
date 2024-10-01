package com.bigdata.Bai5;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Bai5 {

    public static class MMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private DoubleWritable billAmount = new DoubleWritable();
        private Text householdId = new Text("Electricity bill sum: ");

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().trim().split(",");
            if (parts.length == 2) {
                String id = parts[0].trim();
                int consumption = Integer.parseInt(parts[1].trim());
                double bill = calculateBill(consumption);
//                householdId.set(id);
                billAmount.set(bill);
                context.write(householdId, billAmount);
            }
        }

        private double calculateBill(int consumption) {
            double total = 0.0;
            if (consumption > 0) {
                if (consumption <= 100) {
                    total += consumption * 1.734;
                } else if (consumption <= 200) {
                    total += 100 * 1.734 + (consumption - 100) * 2.014;
                } else if (consumption <= 300) {
                    total += 100 * 1.734 + 100 * 2.014 + (consumption - 200) * 2.536;
                } else {
                    total += 100 * 1.734 + 100 * 2.014 + 100 * 2.536 + (consumption - 300) * 2.834;
                }
            }
            return total;
        }
    }

    public static class RReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable totalBill = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            totalBill.set(sum);
            context.write(key, totalBill);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MapReduce");
        job.setJarByClass(Bai5.class);
        job.setMapperClass(MMapper.class);
        job.setReducerClass(RReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
