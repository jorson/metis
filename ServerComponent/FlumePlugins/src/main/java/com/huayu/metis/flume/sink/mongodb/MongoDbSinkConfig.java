package com.huayu.metis.flume.sink.mongodb;

import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Administrator on 14-4-21.
 */
public class MongoDbSinkConfig {

    private Context context;

    private static final Logger LOG = LoggerFactory.getLogger(MongoDbSinkConfig.class);

    //默认的配置项
    private static final long defaultBatchSize = 200;
    private static final long defaultCallTimeout = 10000;
    private static final String defaultCollectionName = "ApiMonitor";

    public volatile String MongoHost;          //MongoDb的主机
    public volatile Integer MongoPort;          //MongoDb的端口
    public volatile String MongoDbName;        //MongoDb的数据库名
    public volatile String CollectionName;     //MongoDb的数据表名称
    public volatile long BatchSize;            //批量写入事件量
    public long CallTimeout;                   //调用超时时间

    public MongoDbSinkConfig(Context context){
        this.context = context;
        build();
    }

    private void build(){
        MongoHost = Preconditions.checkNotNull(context.getString("mongo.host"), "mongo.host is required");
        MongoPort = Preconditions.checkNotNull(context.getInteger("mongo.port"), "mongo.port is required");
        MongoDbName = Preconditions.checkNotNull(context.getString("mongo.database"), "mongo.host is required");

        BatchSize = context.getLong("mongo.batchSize", defaultBatchSize);
        CollectionName = context.getString("mongo.collection", defaultCollectionName);
        CallTimeout = context.getLong("mongo.callTimeout", defaultCallTimeout);
    }
}
