package com.huayu.metis.storm.spout.mongo;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import com.huayu.metis.storm.config.ApiMonitorConfig;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Administrator on 14-4-29.
 */
public abstract class MongoSpoutBase extends BaseRichSpout {

    private static Logger LOG = LoggerFactory.getLogger(MongoSpoutBase.class);

    protected String url;
    protected String dbName;
    protected String collectionName;

    protected BasicDBObject query;
    protected MongoObjectGrabber mapper;
    protected Map<String, MongoObjectGrabber> fields;

    protected Map conf;
    protected TopologyContext context;
    protected SpoutOutputCollector collector;
    //接收消息的队列
    protected LinkedBlockingQueue<DBObject> queue = new LinkedBlockingQueue<DBObject>(10000);

    private MongoSpoutTask spoutTask;


    public MongoSpoutBase(Class<? extends MongoObjectGrabber> mapperClass) throws Exception {
        this.mapper = mapperClass.newInstance();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //声明输出字段
        outputFieldsDeclarer.declare(new Fields(this.mapper.fields()));
    }

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        //将几个保护变量赋值
        this.conf = conf;
        this.context = topologyContext;
        this.collector = spoutOutputCollector;

        //开启获取Mongo数据的执行器
        this.spoutTask = new MongoSpoutTask(this.queue, this.url, this.dbName, this.collectionName, this.query);
        //开启线程
        Thread thread = new Thread(this.spoutTask);
        thread.start();
    }

    @Override
    public void nextTuple() {
        processNextTuple();
    }

    @Override
    public void close() {
        //停止读取进程
        this.spoutTask.stopThread();
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }

    protected abstract void processNextTuple();
}
