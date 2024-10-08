package com.bigdata.BaiTapMapreduce.Bai2;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Bai2b {
    public static class MMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Map<String, Double> productSalesMap = new HashMap<>(); // Bản đồ lưu trạng thái biến nhớ
        private Text productID = new Text();
        private DoubleWritable salesAmount = new DoubleWritable();

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (fields.length == 4) {
                String productId = fields[1].trim();
                double unitPrice = Double.parseDouble(fields[2].trim());
                int quantity = Integer.parseInt(fields[3].trim());

                double sales = unitPrice * quantity;

                productSalesMap.put(productId, productSalesMap.getOrDefault(productId, 0.0) + sales);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Double> entry : productSalesMap.entrySet()) {
                System.out.println("TOILACUBE");
                productID.set(entry.getKey());
                salesAmount.set(entry.getValue());
                context.write(productID, salesAmount);
            }
        }
    }

    public static class RReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sales count");
        job.setJarByClass(Bai2b.class);
        job.setMapperClass(MMapper.class);
        job.setReducerClass(RReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
