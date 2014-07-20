package com.huayu.metis.storm.spout.mongo;

import com.mongodb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 这个是一个不会阻塞的队列, 确保消息会被不断的从数据库中读取出来
 * Created by Administrator on 14-4-29.
 */
public class MongoSpoutTask implements Callable<Boolean>, Runnable, Serializable {

    private static final long serialVersionUID = 4440209304544126477L;
    private static Logger logger = LoggerFactory.getLogger(MongoSpoutTask.class);

    private LinkedBlockingQueue<DBObject> queue;
    private Mongo mongo;
    private DB db;
    private DBCollection collection;
    private DBCursor cursor;

    private AtomicBoolean running = new AtomicBoolean(true);
    private String collectionName;
    private DBObject query;

    public MongoSpoutTask(LinkedBlockingQueue<DBObject> queue, String url,
                          String dbName, String collectionName, DBObject query)
    {
        this.queue = queue;
        this.collectionName = collectionName;
        this.query = query;

        initMongo(url, dbName);
    }

    @Override
    public Boolean call() throws Exception {
        if(this.collectionName == null){
            throw new Exception("Could not locate any of the collections provided or not capped collection");
        }
        this.collection = this.db.getCollection(this.collectionName);
        this.cursor = this.collection.find(query)
                .sort(new BasicDBObject("$natural", 1))
                .addOption(Bytes.QUERYOPTION_TAILABLE)
                .addOption(Bytes.QUERYOPTION_AWAITDATA)
                .addOption(Bytes.QUERYOPTION_NOTIMEOUT);

        //当进程处于运行状态
        while (running.get()) {
            try {
                //检查集合中是否存在记录
                if(this.cursor.hasNext()) {
                    if(logger.isDebugEnabled()) {
                        logger.debug("Fetching a new item from MongoDB cursor");
                    }
                    DBObject obj = this.cursor.next();
                    //将记录推入队列中
                    this.queue.put(obj);
                } else {
                    //进程休息一段时间
                    Thread.sleep(100);
                }
            }  catch (Exception e) {
                if (running.get()) {
                    throw new RuntimeException(e);
                }
            }
        }
        //总是返回True
        return true;
    }

    @Override
    public void run() {
        try{
            call();
        } catch (Throwable ex) {
            logger.error("[hy]error has occur", ex);
        }
    }

    public void stopThread() {
        running.set(false);
    }

    private void initMongo(String url, String dbName) {
        //打开新的Mongo连接
        try{
            MongoClientURI uri = new MongoClientURI(url);
            //创建新的Mongo实例
            mongo = new MongoClient(uri);
            //获取数据库
            db = mongo.getDB(dbName == null ? uri.getDatabase() : dbName);
            //如果是需要进行身份验证的
            if(uri.getUsername() != null){
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
