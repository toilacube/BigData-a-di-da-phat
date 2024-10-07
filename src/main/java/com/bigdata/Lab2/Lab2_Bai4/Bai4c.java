package com.bigdata.Lab2.Lab2_Bai4;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Bai4c {
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

    public static class RReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double minPrice = Double.MAX_VALUE;
            double maxPrice = Double.MIN_VALUE;

            // Iterate through all prices to find the min and max
            for (DoubleWritable value : values) {
                double price = value.get();
                if (price < minPrice) {
                    minPrice = price;
                }
                if (price > maxPrice) {
                    maxPrice = price;
                }
            }

            // Write the result: item name, min price, and max price
            context.write(key, new Text(String.format("Min Price: %.2f, Max Price: %.2f", minPrice, maxPrice)));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "MinMax Price");
        job.setJarByClass(Bai4c.class);
        job.setMapperClass(MMapper.class);
        job.setReducerClass(RReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
