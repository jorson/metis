package com.huayu.metis.keyvalue.trend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 14-7-11.
 */
public abstract class TotalTrendKey implements WritableComparable<TotalTrendKey> {

    protected LongWritable startDate = new LongWritable(0L);
    protected LongWritable endDate = new LongWritable(0L);
    protected IntWritable periodType = new IntWritable(0);
    protected IntWritable appId = new IntWritable(0);
    protected IntWritable terminalCode = new IntWritable(1001);
    protected IntWritable trendsType;

    @Override
    public int compareTo(TotalTrendKey o) {
        int compare = startDate.compareTo(o.startDate);
        if(compare != 0)
            return compare;
        compare = endDate.compareTo(o.endDate);
        if(compare != 0)
            return compare;
        compare = periodType.compareTo(o.periodType);
        if(compare != 0)
            return compare;
        compare = appId.compareTo(o.appId);
        if(compare != 0)
            return compare;
        compare = terminalCode.compareTo(o.terminalCode);
        if(compare != 0)
            return compare;
        return Integer.compare(trendsType.get(), o.trendsType.get());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        startDate.write(dataOutput);
        endDate.write(dataOutput);
        periodType.write(dataOutput);
        appId.write(dataOutput);
        terminalCode.write(dataOutput);
        trendsType.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        startDate.readFields(dataInput);
        endDate.readFields(dataInput);
        periodType.readFields(dataInput);
        appId.readFields(dataInput);
        terminalCode.readFields(dataInput);
        trendsType.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        int result = 1, prim = 99;
        result = prim + periodType.get()
                + appId.get()
                + terminalCode.get()
                + trendsType.get();
        return result;
    }

    @Override
    public String toString() {
        return String.format("%d,%d,%d,%d,%d,%d", startDate.get(),

                endDate.get(), periodType.get(), appId.get(), terminalCode.get(),
                trendsType.get());
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

    public IntWritable getTrendsType() {
        return trendsType;
    }

    public void setTrendsType(IntWritable trendsType) {
        this.trendsType = trendsType;
    }
}
