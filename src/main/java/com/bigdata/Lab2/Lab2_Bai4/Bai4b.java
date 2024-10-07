package com.bigdata.Lab2.Lab2_Bai4;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Bai4b {
    public static class MMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            CSVParser parser = new CSVParserBuilder().withSeparator(',').withQuoteChar('"').build();

            String[] parts = parser.parseLine(line);

            if (parts[0].equals("ten") && parts[1].equals("gia")) {
                return;
            }
            String itemName = parts[0];
            double price = Double.parseDouble(parts[1]);

            // Emit the item name and price
            context.write(new Text(itemName), new DoubleWritable(price));
        }
    }

    public static class RReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;

            // Sum the prices and count the number of entries for each item
            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }

            // Calculate the average price
            double average = sum / count;

            // Write the item and its average price
            context.write(key, new DoubleWritable(average));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Average Price");
        job.setJarByClass(Bai4b.class);
        job.setMapperClass(MMapper.class);
        job.setReducerClass(RReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
