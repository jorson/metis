package com.huayu.metis.job;

import com.huayu.metis.config.UserOnlinePackageConfig;
import com.mongodb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.text.SimpleDateFormat;

/**
 * Created by Administrator on 14-7-30.
 */
public abstract class BasicJob {

    private static final Logger logger = LoggerFactory.getLogger(BasicJob.class);

    protected Mongo mongo;
    protected DB db;
    protected DBCollection collection;

    protected static SimpleDateFormat format=new SimpleDateFormat("yyyyMMdd");

    /**
     * 初始化MONGO数据库
     */
    protected void initMongo() {
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
            //初始化Collection对象
            String collectionName = UserOnlinePackageConfig.getInstance()
                    .tryGet(UserOnlinePackageConfig.CONTROL_NAME);
            collection = db.getCollection(collectionName);
        } catch (UnknownHostException e) {
            // Log the error
            logger.error("Unknown host for Mongo DB", e);
            // Die fast
            throw new RuntimeException(e);
        }
    }

    public abstract int runJob(String[] args);

    protected abstract void loadJobConfig(String configPath);
}
