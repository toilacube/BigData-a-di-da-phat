package com.bigdata.Lab2.Lab2_Bai4;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
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



public class Bai4a {

    public static class MMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static double thresholdPrice;

        protected void setup(Context context) throws IOException, InterruptedException {
            thresholdPrice = Double.parseDouble(context.getConfiguration().get("thresholdPrice"));
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            CSVParser parser = new CSVParserBuilder().withSeparator(',').withQuoteChar('"').build();

            String[] parts = parser.parseLine(line);

            if (parts[0].equals("ten") && parts[1].equals("gia")) {
                return;
            }

            String itemName = parts[0];
            double price = Double.parseDouble(parts[1]);
            String updateDate = parts[3];

            if (price > thresholdPrice) {
                context.write(new Text(updateDate), new IntWritable(1));
            }
        }
    }

    public static class RReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("thresholdPrice", args[2]); // input price

        Job job = Job.getInstance(conf, "Price Filter");
        job.setJarByClass(Bai4a.class);
        job.setMapperClass(MMapper.class);
        job.setReducerClass(RReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
