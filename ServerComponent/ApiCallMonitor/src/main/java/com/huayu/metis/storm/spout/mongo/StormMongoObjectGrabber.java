package com.huayu.metis.storm.spout.mongo;

import backtype.storm.tuple.Tuple;
import com.mongodb.DBObject;

import java.io.Serializable;

/**
 * Created by Administrator on 14-4-29.
 */
public abstract class StormMongoObjectGrabber implements Serializable {

    private static final long serialVersionUID = 333640560466463063L;

    public abstract DBObject map(DBObject object, Tuple tuple);
}

