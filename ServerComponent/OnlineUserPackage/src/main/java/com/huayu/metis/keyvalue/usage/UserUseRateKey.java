package com.huayu.metis.keyvalue.usage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by jorson on 14-7-30.
 */
public class UserUseRateKey implements WritableComparable<UserUseRateKey>, DBWritable {

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
    public int compareTo(UserUseRateKey that) {
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
        compare = normItemKey.compareTo(that.normItemKey);
        if(compare != 0)
            return compare;
        return periodType.compareTo(that.periodType);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.startDate.write(dataOutput);
        this.endDate.write(dataOutput);
        this.appId.write(dataOutput);
        this.terminalCode.write(dataOutput);
        this.periodType.write(dataOutput);
        this.normItemKey.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.startDate.readFields(dataInput);
        this.endDate.readFields(dataInput);
        this.appId.readFields(dataInput);
        this.terminalCode.readFields(dataInput);
        this.periodType.readFields(dataInput);
        this.normItemKey.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        long result = startDate.get();
        result = 31 * result + endDate.get();
        result = 31 * result + periodType.get();
        result = 31 * result + appId.get();
        result = 31 * result + terminalCode.get();
        result = 31 * result + normItemKey.get();
        return Long.valueOf(result).intValue();
    }

    //    @Override
//    public int hashCode() {
//        int result = 1, prim = 75;
//        result = prim + appId.get() + terminalCode.get() + normItemKey.get() + periodType.get();
//        return result;
//    }

    @Override
    public String toString() {
        return String.format("%d,%d,%d,%d,%d,%d", startDate.get(),
                endDate.get(), periodType.get(), appId.get(), terminalCode.get(), normItemKey.get());
    }

    public void setStartDate(LongWritable startDate) {
        this.startDate = startDate;
    }


    public void setEndDate(LongWritable endDate) {
        this.endDate = endDate;
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


    public void setTerminalCode(IntWritable terminalCode) {
        this.terminalCode = terminalCode;
    }

    public void setNormItemKey(int normItemKey) {
        this.normItemKey.set(normItemKey);
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setDate(1, new Date(this.startDate.get()));
        statement.setDate(2, new Date(this.endDate.get()));
        statement.setInt(3, this.periodType.get());
        statement.setInt(4, this.appId.get());
        statement.setInt(5, this.terminalCode.get());
        statement.setInt(6, this.normItemKey.get());
        statement.setInt(8, 0);//TrendsType的默认值，目前只有这一种Type
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.startDate.set(resultSet.getDate("StartDate").getTime());
        this.endDate.set(resultSet.getDate("EndDate").getTime());
        this.periodType.set(resultSet.getInt("PeriodType"));
        this.appId.set(resultSet.getInt("AppId"));
        this.terminalCode.set(resultSet.getInt("TerminalCode"));
        this.normItemKey.set(resultSet.getInt("PageNum"));
    }
}
