package com.huayu.metis.keyvalue.usage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 14-7-21.
 */
public class PageVisitKey implements WritableComparable<PageVisitKey> {

    protected LongWritable startDate;
    protected LongWritable endDate;
    protected IntWritable appId;
    protected IntWritable terminalCode;
    protected Text visitUrl;

    public PageVisitKey(){
        this.startDate = new LongWritable(0);
        this.endDate = new LongWritable(0);
        this.appId = new IntWritable(0);
        this.terminalCode = new IntWritable(0);
        this.visitUrl = new Text();
    }

    @Override
    public int compareTo(PageVisitKey that) {
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
        return this.visitUrl.toString().equalsIgnoreCase(that.visitUrl.toString()) ? 0 : 1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        startDate.write(dataOutput);
        endDate.write(dataOutput);
        appId.write(dataOutput);
        terminalCode.write(dataOutput);
        visitUrl.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        startDate.readFields(dataInput);
        endDate.readFields(dataInput);
        appId.readFields(dataInput);
        terminalCode.readFields(dataInput);
        visitUrl.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        int result = 1, prim = 99;
        result = prim + appId.get() + terminalCode.get() + visitUrl.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("%d,%d,%d,%d,%s", startDate.get(),
                endDate.get(), appId.get(), terminalCode.get(),
                visitUrl.toString());
    }

    public void setStartDate(LongWritable startDate) {
        this.startDate = startDate;
    }

    public void setEndDate(LongWritable endDate) {
        this.endDate = endDate;
    }

    public void setAppId(IntWritable appId) {
        this.appId = appId;
    }

    public void setTerminalCode(IntWritable terminalCode) {
        this.terminalCode = terminalCode;
    }

    public void setVisitUrl(Text visitUrl) {
        this.visitUrl = visitUrl;
    }
}
