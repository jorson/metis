package com.huayu.metis.keyvalue.usage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 14-7-21.
 */
public class PageVisitValue implements Writable {

    private IntWritable visitTimes;
    private LongWritable visitUserId;

    public PageVisitValue() {
        visitTimes = new IntWritable(0);
        visitUserId = new LongWritable(0);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        visitTimes.write(dataOutput);
        visitUserId.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        visitTimes.readFields(dataInput);
        visitUserId.readFields(dataInput);
    }

    @Override
    public String toString() {
        return String.format("%d,%d", visitTimes.get(), visitUserId.get());
    }

    public IntWritable getVisitTimes() {
        return visitTimes;
    }

    public void setVisitTimes(int visitTimes) {
        this.visitTimes.set(visitTimes);
    }

    public LongWritable getVisitUserId() {
        return visitUserId;
    }

    public void setVisitUserId(long visitUserId) {
        this.visitUserId.set(visitUserId);
    }
}
