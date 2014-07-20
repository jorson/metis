package com.huayu.metis.flume.sink;

import com.huayu.metis.flume.sink.mongodb.MongoDbSink;
import com.huayu.metis.flume.utility.CsvUtils;
import com.huayu.metis.flume.utility.KafkaFlumeConstans;
import org.apache.flume.*;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.annotation.meta.field;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 14-4-21.
 */
public class TestMongoSink {

    private static final Logger LOG = LoggerFactory.getLogger(TestMongoSink.class);

    private MongoDbSink sink;
    private Context context;
    private Calendar calendar;
    private int messageCount = 10;

    private final String[] eventFields = new String[] {
        "writeDate", "appId", "logDate", "url", "responseCode", "responseTime", "input", "output"
    };

    @Before
    public void setUp(){
        context = new Context();
        context.put("mongo.host", "mongodb.server.ty.nd");
        context.put("mongo.port", "27017");
        context.put("mongo.database", "api_monitor_dev");
        context.put("mongo.collection", "ApiMonitor");

        calendar = Calendar.getInstance();
        sink = new MongoDbSink();
        sink.setName("MongoSink-" + UUID.randomUUID().toString());
    }

    @After
    public void clear(){
        sink = null;
    }

    public void regexTest() {
        String regexRaw = "\\w*";
        Pattern.compile(regexRaw);
    }

    public void lifeCycle() {
        LOG.debug("Starting...");
        System.out.println("Starting...");

        Configurables.configure(sink, context);
        sink.setChannel(new MemoryChannel());
        sink.start();
        sink.stop();
    }

    public void writeMutilLog() throws InterruptedException, EventDeliveryException {
        LOG.debug("Starting...");
        System.out.println("Starting...");

        Configurables.configure(sink, context);

        Channel channel = new MemoryChannel();
        Configurables.configure(channel, context);

        sink.setChannel(channel);
        sink.start();

        System.out.println("Start put message to Channel");
        Transaction txn = channel.getTransaction();
        txn.begin();
        //将事件推出到通道
        for(int i=0; i<messageCount; i++) {
            Event event = makeApiMonitorEvent();
            Thread.sleep(300);
            channel.put(event);
            System.out.print(i);
        }
        txn.commit();
        txn.close();
        System.out.println("Put message to Channel Over");
        //开始处理
        sink.process();
        //停止
        sink.stop();
    }

    private Event makeApiMonitorEvent() {
        long currentTime = calendar.getTimeInMillis();
        Random rnd = new Random();
        int appId = rnd.nextInt();

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(KafkaFlumeConstans.HEADER_TOPIC_KEY, "api.monitor.test");
        headers.put(KafkaFlumeConstans.HEADER_TIMESTAMP_KEY, String.valueOf(currentTime));
        headers.put(KafkaFlumeConstans.HEADER_FIELDS_KEY, CsvUtils.join(eventFields));

        StringBuilder bodyBuilder = new StringBuilder();
        bodyBuilder.append(currentTime).append(",")
                .append(appId).append(",")
                .append(currentTime).append(",")
                .append("/test/url").append(",")
                .append(rnd.nextInt()).append(",")
                .append(rnd.nextLong()).append(",")
                .append(rnd.nextLong()).append(",")
                .append(rnd.nextLong());

        Event event = new SimpleEvent();
        event.setHeaders(headers);
        event.setBody(bodyBuilder.toString().getBytes());

        return event;
    }
}
