package com.huayu.metis.flume.utility;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ND on 14-4-11.
 */
public class MongodbGenericDao {
    private Mongo mongoEngine = null;
    private DB mongoDB = null;
    private String host;
    private int port;
    private String dbName;

    public MongodbGenericDao(String host, int port, String dbName) {
        this.host = host;
        this.port = port;
        this.dbName = dbName;
    }

    public int add(String tableName, Map<String, Object>... datas) throws UnknownHostException {
        //获取DBCollection；如果默认没有创建，mongodb会自动创建
        DBCollection dbCollection = this.getMongoDB().getCollection(tableName);
        return dbCollection.insert(convertMap2DBObjectArray(datas)).getN();
    }

    public int bulkAdd(String tableName, List<Map<String, Object>> datas) throws UnknownHostException {
        DBCollection dbCollection = this.getMongoDB().getCollection(tableName);
        return dbCollection.insert(convertMap2DBObjectList(datas)).getN();
    }

    public void release() {
        if (mongoEngine != null) {
            mongoEngine.close();
        }
        mongoEngine = null;
        mongoDB = null;
    }

    public Mongo getMongoEngine() throws UnknownHostException {
        if (mongoEngine == null) {
            mongoEngine = new Mongo(this.host, this.port);
        }
        return mongoEngine;
    }

    public void setMongoEngine(Mongo mongoEngine) {
        this.mongoEngine = mongoEngine;
    }

    public DB getMongoDB() throws UnknownHostException {
        if (mongoDB == null) {
            mongoDB = this.getMongoEngine().getDB(this.dbName);
        }
        return mongoDB;
    }

    public void setMongoDB(DB mongoDB) {
        this.mongoDB = mongoDB;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    protected List<DBObject> convertMap2DBObjectList(List<Map<String, Object>> dataList) {
        List resultList = new ArrayList(dataList.size());
        for (int i = 0; i < dataList.size(); i++) {
            Map<String, Object> data = dataList.get(i);
            DBObject obj = new BasicDBObject();
            obj.putAll(data);
            resultList.add(obj);
        }
        return resultList;
    }

    protected List<DBObject> convertMap2DBObjectArray(Map<String, Object>[] dataList) {
        List resultList = new ArrayList(dataList.length);
        for (int i = 0; i < dataList.length; i++) {
            Map<String, Object> data = dataList[i];
            DBObject obj = new BasicDBObject();
            obj.putAll(data);
            resultList.add(obj);
        }
        return resultList;
    }

    protected List<Map<String, Object>> convertDBObject2MapList(List<DBObject> dataList) {
        List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>(dataList.size());
        for (int i = 0; i < dataList.size(); i++) {
            DBObject data = dataList.get(i);
            resultList.add(data.toMap());
        }
        return resultList;
    }
}
