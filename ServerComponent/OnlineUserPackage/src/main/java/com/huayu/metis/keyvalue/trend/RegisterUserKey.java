package com.huayu.metis.keyvalue.trend;

import org.apache.hadoop.io.IntWritable;

/**
 * Created by Administrator on 14-7-11.
 */
public class RegisterUserKey extends TotalTrendKey {

    public RegisterUserKey() {
        this.trendsType = new IntWritable(0);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof RegisterUserKey) {
            RegisterUserKey that = (RegisterUserKey)obj;
            return this.hashCode() == that.hashCode();
        }
        return false;
    }
}
