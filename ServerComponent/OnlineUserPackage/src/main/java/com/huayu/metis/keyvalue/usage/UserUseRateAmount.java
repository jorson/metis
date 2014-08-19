package com.huayu.metis.keyvalue.usage;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by jorson on 14-7-30.
 */
public class UserUseRateAmount extends IntWritable implements DBWritable {
    public UserUseRateAmount(){
        super();
    }
    public UserUseRateAmount(int value) {
        super(value);
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setLong(7, this.get());
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.set(resultSet.getInt("TrendsValue"));
    }
}
