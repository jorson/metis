package com.huayu.metis.keyvalue.usage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 14-7-22.
 */
public class UseRateKey implements WritableComparable<UseRateKey> {

    //访问的日期
    protected LongWritable visitDate;
    //访问的appId
    protected IntWritable appId;
    //访问的终端类型
    protected IntWritable terminalCode;
    //访问次数区间的类型
    /**
     *
     * 按日统计时, 访问次数指标: 1-2:100; 3-5:101; 6-9:102; 10-29:103; 30-49:104; 50+:105
     */
    protected IntWritable normItemKey;


    public UseRateKey() {
        visitDate = new LongWritable(0);
        appId = new IntWritable(0);
        terminalCode = new IntWritable(0);
        normItemKey = new IntWritable(0);
    }

    @Override
    public int hashCode() {
        long result = visitDate.get();
        result = 31 * result + appId.get();
        result = 31 * result + terminalCode.get();
        result = 31 * result + normItemKey.get();
        return Long.valueOf(result).intValue();
    }

    @Override
    public int compareTo(UseRateKey o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    public void setVisitDate(long visitDate) {
        this.visitDate.set(visitDate);
    }

    public void setAppId(int appId) {
        this.appId.set(appId);
    }

    public void setTerminalCode(int terminalCode) {
        this.terminalCode.set(terminalCode);
    }

    public void setNormItemKey(int normItemKey) {
        this.normItemKey.set(normItemKey);
    }
}
