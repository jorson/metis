package com.huayu.metis.flume.sink.mongodb.writer;

import com.huayu.metis.flume.sink.mongodb.MongoDbSinkConfig;
import com.huayu.metis.flume.utility.CsvUtils;
import com.huayu.metis.flume.utility.KafkaFlumeConstans;
import com.huayu.metis.flume.utility.MongodbGenericDao;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.instrumentation.SinkCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 14-4-21.
 */
public class MongoDbWriter implements SinkWriter {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDbWriter.class);

    private final MongoDbSinkConfig sinkConfig;
    private final SinkCounter counter;
    private final MongodbGenericDao mongodbGenericDao;

    private List<Map<String, Object> > buffedEventDatas;
    private final int defaultInnerBatchSize = 200;
    private int innerBatchSize; //内部批量数, 当缓冲的数据量到达这个值的时候, Writer将强制Sync
    private int innerCounter = 0;

    public MongoDbWriter(MongoDbSinkConfig config, SinkCounter counter) {
        this.sinkConfig = config;
        this.counter = counter;
        this.mongodbGenericDao = new MongodbGenericDao(sinkConfig.MongoHost, sinkConfig.MongoPort, sinkConfig.MongoDbName);

        buffedEventDatas = new ArrayList<Map<String, Object>>();

        this.counter.incrementConnectionCreatedCount();
    }

    @Override
    public void open(String filePath) throws IOException {
        throw new UnsupportedOperationException("[hy]Mongo Writer not support open");
    }

    @Override
    public void append(final Event event) {
        try {
            //如果已经写入的数据要小于BatchSize的话
            if(innerCounter < innerBatchSize) {
                Map<String, Object> datas = parseEventToData(event);
                if(datas != null){
                    buffedEventDatas.add(datas);
                    innerCounter ++;
                    this.counter.incrementEventDrainAttemptCount();
                }
            } else {  //已经达到需要写入数据库的计数
                sync();
            }
        } catch (IOException e) {
            LOG.error("[hy]parse Message Error", e);
            e.printStackTrace();
        }
    }

    @Override
    public void sync() throws IOException {
        synchronized (buffedEventDatas) {
            if(buffedEventDatas.size() > 0) {
                mongodbGenericDao.bulkAdd(sinkConfig.CollectionName, buffedEventDatas);
                //初始化计数器
                buffedEventDatas.clear();
                innerCounter = 0;
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            mongodbGenericDao.release();
            this.counter.incrementConnectionClosedCount();
        } catch (Exception ex) {
            LOG.error("[hy]Close Mongo Writer Error", ex);
        }
    }

    @Override
    public boolean isUnderReplicated() {
        return false;
    }

    @Override
    public void configure(Context context) {
        //获取配置的innerBatchSize
        innerBatchSize = context.getInteger("mongo.syncBatchSize", defaultInnerBatchSize);
    }

    /**
     * 将事件中的数据转成要写入Mongo的数据
     * @param event 事件
     * @return 待写入数据
     */
    private Map<String, Object> parseEventToData(Event event){
        Map<String, String> headers = event.getHeaders();
        //如果事件头没有包含字段说明
        if(!headers.containsKey(KafkaFlumeConstans.HEADER_FIELDS_KEY)){
            LOG.debug("[hy]Event not Contain Fields Description");
            return null;
        }

        try{
            String field = headers.get(KafkaFlumeConstans.HEADER_FIELDS_KEY);
            String message = new String(event.getBody(), KafkaFlumeConstans.DEFAULT_ENCODING);

            if (message != null) {
                String[] fields = CsvUtils.split(field);
                String[] messages = CsvUtils.split(message);

                if (fields.length != messages.length) {
                    LOG.debug("[hy]Field Size not equal to Message Size");
                    return null;
                }

                Map<String, Object> datas = new HashMap<String, Object>();
                for(int i=0; i<fields.length; i++) {
                    datas.put(fields[i], messages[i]);
                }
                return datas;
            }
            LOG.debug("[hy]Event Message is NULL");
            return null;
        } catch (UnsupportedEncodingException e) {
            LOG.error("[hy]parse Message Error", e);
            return null;
        }
    }
}
