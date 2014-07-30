package com.huayu.metis.keyvalue.usage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 页面访问情况的输出键
 * Created by Administrator on 14-7-30.
 */
public class PageVisitOutputValue implements Writable, DBWritable {

    private IntWritable visitTimes;
    private IntWritable visitUsers;

    public PageVisitOutputValue() {
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

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setInt(7, this.visitTimes.get());
        statement.setInt(8, this.visitUsers.get());
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.visitTimes.set(resultSet.getInt("VisitTimes"));
        this.visitUsers.set(resultSet.getInt("UserAmount"));
    }

    public void setVisitTimes(IntWritable visitTimes) {
        this.visitTimes = visitTimes;
    }

    public void setVisitUsers(IntWritable visitUsers) {
        this.visitUsers = visitUsers;
    }
}
