package com.huayu.metis.keyvalue.trend;

/**
 * 新增用户的Key
 * Created by Administrator on 14-7-11.
 */
public class AddedUserKey extends TotalTrendKey {

    public AddedUserKey() {
        super.trendsType.set(1);
        //这是一个变化
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof AddedUserKey) {
            RegisterUserKey that = (RegisterUserKey)obj;
            return this.hashCode() == that.hashCode();
        }
        return false;
    }
}
