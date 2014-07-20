package com.huayu.metis.keyvalue;

import org.apache.hadoop.io.IntWritable;

/**
 * Created by Administrator on 14-7-11.
 */
public class ActiveUserKey extends TotalTrendKey {

    public ActiveUserKey() {
        this.trendsType = new IntWritable(2);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof ActiveUserKey) {
            RegisterUserKey that = (RegisterUserKey)obj;
            return this.hashCode() == that.hashCode();
        }
        return false;
    }
}
