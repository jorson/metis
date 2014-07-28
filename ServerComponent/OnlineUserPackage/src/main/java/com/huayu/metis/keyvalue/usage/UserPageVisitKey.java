package com.huayu.metis.keyvalue.usage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 用户页面访问情况的键
 * Created by Administrator on 14-7-28.
 */
public class UserPageVisitKey implements WritableComparable<UserPageVisitKey> {

    private LongWritable statDate;
    private IntWritable appId;
    private IntWritable terminalCode;
    private LongWritable userId;

    public UserPageVisitKey() {
        this.statDate = new LongWritable(0);
        this.appId = new IntWritable(0);
        this.terminalCode = new IntWritable(0);
        this.userId = new LongWritable(0);
    }

    @Override
    public int compareTo(UserPageVisitKey that) {
        int compare = statDate.compareTo(that.statDate);
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
        statDate.write(dataOutput);
        appId.write(dataOutput);
        terminalCode.write(dataOutput);
        userId.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        statDate.readFields(dataInput);
        appId.readFields(dataInput);
        terminalCode.readFields(dataInput);
        userId.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        int result = 1, prim = 77;
        long uId = userId.get();
        while(uId > Integer.MAX_VALUE){
            uId = uId - Integer.MAX_VALUE;
        }
        long sId = statDate.get();
        while (sId > Integer.MAX_VALUE) {
            sId = sId - Integer.MAX_VALUE;
        }

        result = prim + appId.get() + terminalCode.get() + (int)uId + (int)sId;
        return result;
    }

    @Override
    public String toString() {
        return String.format("%d,%d,%d,%d", statDate.get(),
                appId.get(), terminalCode.get(),
                userId.get());
    }

    public void setStatDate(long statDate) {
        this.statDate.set(statDate);
    }

    public void setAppId(int appId) {
        this.appId.set(appId);
    }

    public void setTerminalCode(int terminalCode) {
        this.terminalCode.set(terminalCode);
    }

    public void setUserId(long userId) {
        this.userId.set(userId);
    }
}
