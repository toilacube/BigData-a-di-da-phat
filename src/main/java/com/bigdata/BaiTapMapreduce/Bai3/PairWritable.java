package com.bigdata.BaiTapMapreduce.Bai3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairWritable implements Writable {
    private DoubleWritable first;
    private IntWritable second;

    public PairWritable() {
        this.first = new DoubleWritable();
        this.second = new IntWritable();
    }

    public PairWritable(DoubleWritable first, IntWritable second) {
        this.first = first;
        this.second = second;
    }

    public DoubleWritable getFirst() {
        return first;
    }

    public IntWritable getSecond() {
        return second;
    }

    public void setFirst(DoubleWritable first) {
        this.first = first;
    }

    public void setSecond(IntWritable second) {
        this.second = second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }
}
