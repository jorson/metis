package com.huayu.metis.keyvalue.usage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by jorson on 14-7-30.
 */
public class UserUseRateKey implements WritableComparable<UserUseRateKey> {

    protected LongWritable startDate;
    protected LongWritable endDate;
    protected IntWritable periodType;
    protected IntWritable appId;
    protected IntWritable terminalCode;
    /**
     * 访问次数区间的类型
     * 按日统计时, 访问次数指标: 1-2:100; 3-5:101; 6-9:102; 10-29:103; 30-49:104; 50+:105
     * 按周统计时, 访问次数指标: 1-2:200; 3-5:201; 6-9:202; 10-29:203; 30-49:204; 50+:205
     * 按月统计时, 访问次数指标: 1-2:300; 3-5:301; 6-9:302; 10-29:303; 30-49:304; 50+:305
     */
    protected IntWritable normItemKey;

    public UserUseRateKey() {
        startDate = new LongWritable(0);
        endDate = new LongWritable(0);
        periodType = new IntWritable(0);
        appId = new IntWritable(0);
        terminalCode = new IntWritable(0);
        normItemKey = new IntWritable(0);
    }


    @Override
    public int compareTo(UserUseRateKey o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    public LongWritable getStartDate() {
        return startDate;
    }

    public void setStartDate(LongWritable startDate) {
        this.startDate = startDate;
    }

    public LongWritable getEndDate() {
        return endDate;
    }

    public void setEndDate(LongWritable endDate) {
        this.endDate = endDate;
    }

    public IntWritable getPeriodType() {
        return periodType;
    }

    public void setPeriodType(IntWritable periodType) {
        this.periodType = periodType;
    }

    public IntWritable getAppId() {
        return appId;
    }

    public void setAppId(IntWritable appId) {
        this.appId = appId;
    }

    public IntWritable getTerminalCode() {
        return terminalCode;
    }

    public void setTerminalCode(IntWritable terminalCode) {
        this.terminalCode = terminalCode;
    }

    public IntWritable getNormItemKey() {
        return normItemKey;
    }

    public void setNormItemKey(int normItemKey) {
        this.normItemKey.set(normItemKey);
    }
}
