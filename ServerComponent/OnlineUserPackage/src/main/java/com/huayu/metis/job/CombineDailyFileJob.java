package com.huayu.metis.job;

import com.huayu.metis.config.UserOnlinePackageConfig;
import com.mongodb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;

/**
 * 将每天的文件进行合并的作业
 * Created by Administrator on 14-7-28.
 */
public class CombineDailyFileJob {

    private static Logger logger = LoggerFactory.getLogger(CombineDailyFileJob.class);

    private Mongo mongo;
    private DB db;
    private DBCollection collection;
    private DBCursor cursor;

    public CombineDailyFileJob() {
    }

    private void loadLastExecuteDate() {
        String collectionName = UserOnlinePackageConfig.getInstance()
                .tryGet(UserOnlinePackageConfig.CONTROL_NAME);
        this.collection = this.db.getCollection(collectionName);
        this.cursor = this.collection.find();
        if(this.cursor.hasNext()) {
            DBObject obj = this.cursor.next();
            obj.get("last_execute_date");
        }

    }

    private void initMongo() {

        //打开新的Mongo链接
        try {
            MongoClientURI uri =
                    new MongoClientURI(UserOnlinePackageConfig.getInstance()
                            .tryGet(UserOnlinePackageConfig.CONTROL_URL));
            mongo = new MongoClient(uri);
            db = mongo.getDB(UserOnlinePackageConfig.getInstance()
                    .tryGet(UserOnlinePackageConfig.CONTROL_CATALOG));
            //如果需要进行身份验证
            if(uri.getUsername() != null) {
                db.authenticate(uri.getUsername(), uri.getPassword());
            }
        } catch (UnknownHostException e) {
            // Log the error
            logger.error("Unknown host for Mongo DB", e);
            // Die fast
            throw new RuntimeException(e);
        }
    }
}
