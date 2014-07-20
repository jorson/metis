package com.huayu.metis.storm.spout.mongo;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.huayu.metis.storm.ConstansValues;
import com.huayu.metis.storm.config.ApiMonitorConfig;
import com.huayu.metis.storm.spout.ApiCallDocument;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryOperators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 基于时间的Mongo数据Spout, 设置一个起始时间和间隔, 以起始时间为准,将在指定时间间隔
 * 内的数据作为一个批次输入
 * Created by wuhy on 14-4-30.
 */
public class TimeBaseMongoSpout extends MongoSpoutBase {

    private List<ApiCallDocument> queueMessage = new ArrayList<ApiCallDocument>();
    private String timestampField;
    private long baseTimestamp;
    private int interval;
    private final AtomicInteger locker = new AtomicInteger(1);

    public TimeBaseMongoSpout(Class<? extends MongoObjectGrabber> mapperCls) throws Exception {
        super(mapperCls);
    }

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        //从配置中获取数据源的配置
        this.url = conf.get(ApiMonitorConfig.SOURCE_URL).toString();
        this.dbName = conf.get(ApiMonitorConfig.SOURCE_CATALOG).toString();
        this.collectionName = conf.get(ApiMonitorConfig.SOURCE_NAME).toString();
        this.timestampField = conf.get(ApiMonitorConfig.SOURCE_TIME_STAMP_FIELD).toString();
        this.interval = Integer.parseInt(conf.get(ApiMonitorConfig.SOURCE_GROUP_INTERVAL).toString());

        String baseTimestampFile = conf.get(ApiMonitorConfig.BASE_TIMESTAMP_FILE_PATH).toString();
        try {
            this.baseTimestamp = ApiMonitorConfig.getBaseTimestamp(baseTimestampFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //这里设置数据的查询
        this.query = new BasicDBObject();
        DBObject condition = new BasicDBObject();
        condition.put(QueryOperators.GT, this.baseTimestamp);
        this.query.put(this.timestampField, condition);

        //执行基类的方法
        super.open(conf, topologyContext, spoutOutputCollector);
    }

    @Override
    protected void processNextTuple() {
        //从文档队列中获取一个对象
        DBObject object = this.queue.poll();
        if(object == null) {
            return;
        }
        //从文档中获取时间戳
        Object docObject = this.mapper.map(object);
        if(!(docObject instanceof ApiCallDocument)) {
            return;
        }

        ApiCallDocument apiDocObj = (ApiCallDocument)docObject;
        long timestamp = apiDocObj.getLogDate();
        //如果时间戳小于基准时间, 忽略掉数据
        if(timestamp < baseTimestamp) {
            return;
        }

        synchronized (locker) {
            //如果当前时间戳-基准时间戳 > 间隔时间; 达到发出的条件
            if(timestamp - this.baseTimestamp >= this.interval) {

                    //将消息做一个拷贝
                    List<ApiCallDocument> sendMessage = new ArrayList<ApiCallDocument>(queueMessage);
                    //发出消息
                    this.collector.emit(new Values(this.baseTimestamp, sendMessage));
                    //清空队列
                    queueMessage.clear();
                    //推进Timestamp
                    this.baseTimestamp += this.interval;

            } else {
                queueMessage.add(apiDocObj);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //声明输出的字段
        outputFieldsDeclarer.declare(new Fields(
                ConstansValues.API_MONITOR_TIME_BASE_GROUP_KEY,
                ConstansValues.API_MONITOR_TIME_BASE_OUTPUT_KEY));
    }
}
