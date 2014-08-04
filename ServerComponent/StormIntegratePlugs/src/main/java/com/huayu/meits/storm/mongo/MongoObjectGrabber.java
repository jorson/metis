package com.huayu.meits.storm.mongo;

import com.mongodb.DBObject;
import org.bson.types.ObjectId;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 14-4-29.
 */
public abstract class MongoObjectGrabber<T> implements Serializable {

    protected ObjectId _id;

    private static final long serialVersionUID = 7265794696380763567L;

    public abstract T map(DBObject object);

    public abstract String[] fields();

    public abstract Object getKey();

    public abstract Map<String, Object> getValue();

    public ObjectId get_id(){
        return _id;
    }
}
