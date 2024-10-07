package com.bigdata.Lab2.Lab2_Bai3;

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
import java.util.HashMap;
import java.util.Map;

/*
The Mapper file will have this input format from Bai3a output:
 - (A, count(A))
 - ((A,B), count(A,B))
0	177
0,1	5
0,10	5

In mapper, for each line in input file:
    if line has pair (A,B):
        write (A, "P_B_count(A,B)")
        write (B, "P_A_count(A,B)")
    if line has single number A:
        write (A, "S_count(A)")

Continue in Reducer:
    sum_pairs = {0}
    for value in values:
        val = value.split("_")
        if value[0] == 'S':
            count_key= Integer(val[1])
        if value[0] == 'P':
            sum_pairs[val[1]] += Integer(val[2])
    for pair in sum_pairs:
        write (key, sum_pairs[pair]/count_key)



 */

public class Bai3b {

    public static class CondProb_Mapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(LongWritable longWritable, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");

            if (parts[0].contains(",")) {
                // This is the pair (A,B), split and get count(A,B)
                String[] pair = parts[0].split(",");
                String productA = pair[0];
                String productB = pair[1];
                String countAB = parts[1];

                // write (A, "P_B_count(A,B)")
                outputKey.set(productA);
                outputValue.set("P_" + productB + "_" + countAB);
                context.write(outputKey, outputValue);

                // write (B, "P_A_count(A,B)")
                outputKey.set(productB);
                outputValue.set("P_" + productA + "_" + countAB);
                context.write(outputKey, outputValue);
            } else {
                // This is the single count of product A
                String product = parts[0];
                String countA = parts[1];

                // Emit (A, "S_count(A)")
                outputKey.set(product);
                outputValue.set("S_" + countA);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class CondProb_Reducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int countA = 0;
            HashMap<String, Integer> sum_pairs = new HashMap<>();

            // Iterate over all values for the current key
            for (Text value : values) {
                String[] valParts = value.toString().split("_");

                if (valParts[0].equals("S")) {
                    // This is the single count of A
                    countA = Integer.parseInt(valParts[1]);
                } else if (valParts[0].equals("P")) {
                    // This is the pair (A, B), valParts[1] is B, valParts[2] is count(A, B)
                    String otherProduct = valParts[1];
                    int countAB = Integer.parseInt(valParts[2]);

                    // Update sum_pairs for B
                    sum_pairs.put(otherProduct, sum_pairs.getOrDefault(otherProduct, 0) + countAB);
                }
            }

            // Calculate P(B | A) for each pair (A, B)
            for (String otherProduct : sum_pairs.keySet()) {
                int countAB = sum_pairs.get(otherProduct);
                double probBA = (double) countAB / countA;

                // write the result: key is (A, B), value is "prob(B | A)"
//                context.write(new Text(key.toString() + "," + otherProduct), new Text(String.format("prob(%s|%s)=%.4f", otherProduct, key.toString(), probBA)));

                context.write(new Text(key.toString() + "," + otherProduct),
                        new Text(String.format("count(A)=%d, count(A,B)=%d, prob(%s|%s)=%.4f",
                                countA, countAB, otherProduct, key.toString(), probBA)));
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CondProb");

        job.setJarByClass(Bai3b.class);
        job.setMapperClass(CondProb_Mapper.class);
        job.setReducerClass(CondProb_Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
