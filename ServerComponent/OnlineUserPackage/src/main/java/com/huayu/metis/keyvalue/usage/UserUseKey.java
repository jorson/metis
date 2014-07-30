package com.huayu.metis.keyvalue.usage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 页面被访问频率的Key
 * Created by Administrator on 14-7-22.
 */
public class UserUseKey implements WritableComparable<UserUseKey> {

    protected LongWritable startDate;
    protected LongWritable endDate;
    protected IntWritable periodType;
    protected IntWritable appId;
    protected IntWritable terminalCode;
    protected LongWritable userId;

    public UserUseKey() {
        startDate = new LongWritable(0);
        endDate = new LongWritable(0);
        periodType = new IntWritable(0);
        appId = new IntWritable(0);
        terminalCode = new IntWritable(0);
        userId = new LongWritable(0);
    }

    @Override
    public int compareTo(UserUseKey that) {
        int compare = startDate.compareTo(that.startDate);
        if(compare != 0)
            return compare;
        compare = endDate.compareTo(that.endDate);
        if(compare != 0)
            return compare;
        compare = appId.compareTo(that.appId);
        if(compare != 0)
            return compare;
        compare = terminalCode.compareTo(that.terminalCode);
        if(compare != 0)
            return compare;
        return userId.compareTo(that.userId);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        startDate.write(dataOutput);
        endDate.write(dataOutput);
        appId.write(dataOutput);
        terminalCode.write(dataOutput);
        userId.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        startDate.readFields(dataInput);
        endDate.readFields(dataInput);
        appId.readFields(dataInput);
        terminalCode.readFields(dataInput);
        userId.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        int result = 1, prim = 88;
        long uId = userId.get();
        while(uId > Integer.MAX_VALUE){
            uId = uId - Integer.MAX_VALUE;
        }
        result = prim + appId.get() + terminalCode.get() + (int)uId + periodType.get();
        return result;
    }

    @Override
    public String toString() {
        return String.format("%d,%d,%d,%d,%d,%d", startDate.get(),
                endDate.get(), periodType.get(), appId.get(), terminalCode.get(),
                userId.toString());
    }

    public void setStartDate(long startDate) {
        this.startDate.set(startDate);
    }

    public void setEndDate(long endDate) {
        this.endDate.set(endDate);
    }

    public void setAppId(int appId) {
        this.appId.set(appId);
    }

    public void setTerminalCode(int terminalCode) {
        this.terminalCode.set(terminalCode);
    }

    public void setUserId(int userId) {
        this.userId.set(userId);
    }

    public LongWritable getStartDate() {
        return startDate;
    }

    public LongWritable getEndDate() {
        return endDate;
    }

    public IntWritable getAppId() {
        return appId;
    }

    public IntWritable getTerminalCode() {
        return terminalCode;
    }

    public LongWritable getUserId() {
        return userId;
    }

    public IntWritable getPeriodType() {
        return periodType;
    }

    public void setPeriodType(int periodType) {
        this.periodType.set(periodType);
    }
}
