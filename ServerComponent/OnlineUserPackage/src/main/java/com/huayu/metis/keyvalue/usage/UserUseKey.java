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

    protected LongWritable visitDate;
    protected IntWritable appId;
    protected IntWritable terminalCode;
    protected LongWritable userId;
    protected Text pageUrl;

    public UserUseKey() {
        visitDate = new LongWritable(0);
        appId = new IntWritable(0);
        terminalCode = new IntWritable(0);
        userId = new LongWritable(0);
        pageUrl = new Text();
    }

    @Override
    public int compareTo(UserUseKey o) {
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

    public void setUserId(int userId) {
        this.userId.set(userId);
    }

    public void setPageUrl(Text pageUrl) {
        this.pageUrl = pageUrl;
    }

    public LongWritable getVisitDate() {
        return visitDate;
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
}
