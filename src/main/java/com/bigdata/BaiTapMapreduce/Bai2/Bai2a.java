package com.bigdata.BaiTapMapreduce.Bai2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai2a {
    public static class MMapper extends Mapper<Object, Text, Text, DoubleWritable> {
         private Text productID = new Text();
        private DoubleWritable salesAmount = new DoubleWritable();

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (fields.length == 4) {
                String productId = fields[1].trim();  // Mã sản phẩm
                double unitPrice = Double.parseDouble(fields[2].trim()); // Đơn giá
                int quantity = Integer.parseInt(fields[3].trim()); // Số lượng

                double sales = unitPrice * quantity;

                productID.set(productId);
                salesAmount.set(sales);

                context.write(productID, salesAmount);
            }
        }
    }

    public static class RReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                Context context) throws IOException, InterruptedException {
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
        Job job = Job.getInstance(conf, "Web count");
        job.setJarByClass(Bai2a.class);
        job.setMapperClass(MMapper.class);
        job.setCombinerClass(RReducer.class);
        job.setReducerClass(RReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
