package com.bigdata.Lab2.Lab2_Bai3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class Bai3a {
    /*
    This is the preparation for task b:
    Xác suất mua sản phẩm B khi đã mua sản phẩm A (xác suất có điều
    kiện - conditional probability): prob (B | A) = count (A, B) / count (A).

    Example of a line: 25 52 164 240 274 328 368 448 538 561 630 687 730 775 825 834
    So our mapper output will have to have this key-value structure:
        - (A, 1)
        - ((A,B), 1)
        in order to calculate prob(B | A) in reducer

     Reducer class for each (key, value) pair:
        - (A, count(A))
        - ((A,B), count(A,B))
    Example:
    0	177
    0,1	5
    0,10	5
    This output will come to next mapreduce Bai3b
     */

    public static class Bai3_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text pair = new Text();

        @Override
        public void map(LongWritable longWritable, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] words = line.split(" ");
            int n = words.length;

            for (int i = 0; i < n; i++) {
                word.set(words[i]);
                outputCollector.collect(word, one);

                for (int j = i + 1; j < n; j++) {
                    Long l1 = Long.parseLong(words[i]);
                    Long l2 = Long.parseLong(words[j]);

                    // Ensure the pair is sorted lexicographically
                    if (l1 <= l2) {
                        pair.set(words[i] + "," + words[j]);
                    } else {
                        pair.set(words[j] + "," + words[i]);
                    }
                    outputCollector.collect(pair, one);
                }
            }

            /*
             Example: 25 52 164 240 274 328 368 448 538 561 630 687 730 775 825 834
             first loop: (25, 1), ("25,52", 1), ("25,164", 1)
             */
        }
    }

    public static class Bai3_Reducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            outputCollector.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(Bai3a.class);
        conf.setJobName("WordCount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(Bai3_Mapper.class);
        conf.setReducerClass(Bai3_Reducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
