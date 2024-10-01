package com.bigdata.Bai4;

import com.bigdata.Bai2.Bai2a;
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;

public class Bai4 {
    public static class MMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
        private IntWritable k = new IntWritable(1);
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int val = Integer.parseInt(line);
            double logVal = Math.log(val);
           context.write(k, new DoubleWritable(logVal));
        }
    }

    public static class RReducer
            extends Reducer<IntWritable, DoubleWritable, Text, BigDecimal> {
        private BigDecimal geometricMean = BigDecimal.ZERO;
        private MathContext mathContext = new MathContext(10, RoundingMode.HALF_UP);
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            BigDecimal totalLog = BigDecimal.ZERO;
            int count = 0;

            for (DoubleWritable val : values) {
                totalLog = totalLog.add(BigDecimal.valueOf(val.get()));
                count++;
            }

            if (count > 0) {
                BigDecimal meanLog = totalLog.divide(BigDecimal.valueOf(count), mathContext); // totalLog / count
                geometricMean = BigDecimal.valueOf(Math.E).pow(meanLog.intValue(), mathContext); // e^(meanLog)
                context.write(new Text("Geometric Mean"), geometricMean);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MapReduce");
        job.setJarByClass(Bai4.class);
        job.setMapperClass(MMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
       // job.setCombinerClass(RReducer.class);
        job.setReducerClass(RReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BigDecimal.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
