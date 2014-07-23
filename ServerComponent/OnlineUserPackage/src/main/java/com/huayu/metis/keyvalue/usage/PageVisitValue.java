package com.huayu.metis.keyvalue.usage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 14-7-21.
 */
public class PageVisitValue implements Writable {

    private IntWritable visitTimes;
    private IntWritable visitUsers;

    public PageVisitValue() {
        visitTimes = new IntWritable(0);
        visitUsers = new IntWritable(0);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        visitTimes.write(dataOutput);
        visitUsers.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        visitTimes.readFields(dataInput);
        visitUsers.readFields(dataInput);
    }

    @Override
    public String toString() {
        return String.format("%d,%d", visitTimes.get(), visitUsers.get());
    }

    public IntWritable getVisitTimes() {
        return visitTimes;
    }

    public void setVisitTimes(IntWritable visitTimes) {
        this.visitTimes = visitTimes;
    }

    public IntWritable getVisitUsers() {
        return visitUsers;
    }

    public void setVisitUsers(IntWritable visitUsers) {
        this.visitUsers = visitUsers;
    }
}
