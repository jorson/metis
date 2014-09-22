package com.huayu.metis.keyvalue.usage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
 * Created by Administrator on 14-7-21.
 */
public class PageVisitKey implements WritableComparable<PageVisitKey>, DBWritable {

    protected LongWritable startDate;
    protected LongWritable endDate;
    protected IntWritable appId;
    protected IntWritable terminalCode;
    protected IntWritable periodType;
    protected Text visitUrl;

    public PageVisitKey(){
        this.startDate = new LongWritable(0);
        this.endDate = new LongWritable(0);
        this.appId = new IntWritable(0);
        this.terminalCode = new IntWritable(0);
        this.periodType = new IntWritable(0);
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
        periodType.write(dataOutput);
        appId.write(dataOutput);
        terminalCode.write(dataOutput);
        visitUrl.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        startDate.readFields(dataInput);
        endDate.readFields(dataInput);
        periodType.readFields(dataInput);
        appId.readFields(dataInput);
        terminalCode.readFields(dataInput);
        visitUrl.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        long result = startDate.get();
        result = 31 * result + endDate.get();
        result = 31 * result + appId.get();
        result = 31 * result + terminalCode.get();
        result = 31 * result + periodType.get();
        result = 31 * result + visitUrl.toString().hashCode();
        return Long.valueOf(result).intValue();
    }

//    @Override
//    public int hashCode() {
//        int result = 1, prim = 99;
//        result = prim + appId.get() + terminalCode.get() + visitUrl.hashCode();
//        return result;
//    }

    @Override
    public String toString() {
        return String.format("%d,%d,%d,%d,%d,%s", startDate.get(),
                endDate.get(), periodType.get(), appId.get(), terminalCode.get(),
                visitUrl.toString());
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

    public void setVisitUrl(Text visitUrl) {
        this.visitUrl = visitUrl;
    }

    public IntWritable getAppId() {
        return appId;
    }

    public IntWritable getPeriodType() {
        return periodType;
    }

    public void setPeriodType(int periodType) {
        this.periodType.set(periodType);
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setDate(1, new Date(this.startDate.get()));
        statement.setDate(2, new Date(this.endDate.get()));
        statement.setInt(3, this.periodType.get());
        statement.setInt(4, this.appId.get());
        statement.setInt(5, this.terminalCode.get());
        statement.setString(6, this.visitUrl.toString());
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.startDate.set(resultSet.getDate("StartDate").getTime());
        this.endDate.set(resultSet.getDate("EndDate").getTime());
        this.periodType.set(resultSet.getInt("PeriodType"));
        this.appId.set(resultSet.getInt("AppId"));
        this.terminalCode.set(resultSet.getInt("TerminalCode"));
        this.visitUrl.set(resultSet.getString("PageUrl"));
    }
}
