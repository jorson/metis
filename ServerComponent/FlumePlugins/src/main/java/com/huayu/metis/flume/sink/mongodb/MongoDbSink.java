package com.huayu.metis.flume.sink.mongodb;

import com.huayu.metis.flume.sink.mongodb.writer.MongoDbWriter;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Administrator on 14-4-21.
 */
public class MongoDbSink extends AbstractSink implements Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDbSink.class);

    private Context context;
    private MongoDbSinkConfig config;
    private SinkCounter sinkCounter;
    private MongoDbWriter mongoWriter;

    public MongoDbSink() {
    }

    @Override
    public void configure(Context context) {
        if(config == null){
            config = new MongoDbSinkConfig(context);
        }
        this.context = context;
        this.sinkCounter = new SinkCounter(getName());
    }

    @Override
    public synchronized void start() {
        this.sinkCounter.start();
        this.mongoWriter = new MongoDbWriter(this.config, this.sinkCounter);

        Configurables.configure(this.mongoWriter, context);
        super.start();
    }

    @Override
    public synchronized void stop() {
        LOG.debug("[hy]Begin closing...");
        try{
            //同步还存在缓冲中的数据
            mongoWriter.sync();
            //关闭写写入器
            mongoWriter.close();
        } catch(Exception ex) {
            LOG.warn("Exception while closing Mongo Writer, Exeption follows.", ex);
        }

        mongoWriter = null;
        sinkCounter.stop();
        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction tnx = channel.getTransaction();

        //开始写入事件
        tnx.begin();
        try{
            int txnEventCount = 0;
            for(txnEventCount = 0; txnEventCount < config.BatchSize; txnEventCount++){
                Event event = channel.take();
                //如果已经读取不到事件
                if(event == null){
                    break;
                }
                mongoWriter.append(event);
            }

            if(txnEventCount == 0){
                sinkCounter.incrementBatchEmptyCount();
            } else if(txnEventCount == config.BatchSize) {
                sinkCounter.incrementBatchCompleteCount();
            } else {
                sinkCounter.incrementBatchUnderflowCount();
            }

            //在提交事务前将写入缓冲中所有的数据, 同步到数据库
            mongoWriter.sync();
            tnx.commit();
            if(txnEventCount < 1) {
                return Status.BACKOFF;
            } else {
                sinkCounter.addToEventDrainSuccessCount(txnEventCount);
                return Status.READY;
            }

        } catch (Throwable th) {
            tnx.rollback();
            LOG.error("[hy]process failed", th);
            if(th instanceof Error) {
                throw (Error)th;
            } else {
                throw new EventDeliveryException(th);
            }
        } finally {
            tnx.close();
        }
    }
}
