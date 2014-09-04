package com.huayu.metis.flume.source;

import com.google.common.collect.Lists;
import com.huayu.metis.flume.source.kafka.KafkaSource;
import com.huayu.metis.flume.utility.KafkaFlumeConstans;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.source.AbstractSource;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by Administrator on 14-4-22.
 */
public class KafkaSourceTest {

    /**
     * The constant LOGGER.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSourceTest.class);
    /**
     * The Source.
     */
    private AbstractSource source;
    /**
     * The Channel.
     */
    private Channel channel = new MemoryChannel();
    /**
     * The Context.
     */
    private Context context = new Context();
    /**
     * The Rcs.
     */
    private ChannelSelector rcs = new ReplicatingChannelSelector();

    /**
     * Sets up.
     *
     * @throws Exception              the exception
     */
    @Before
    public void setUp() throws Exception {

        // kafka config
        this.context.put("zookeeper.connect", "192168-205213:2181,192168-205213:2182,192168-205213:2183");
        this.context.put("group.id", "testGroup");
        this.context.put("zookeeper.session.timeout.ms", "400");
        this.context.put("zookeeper.sync.time.ms", "200");
        this.context.put("auto.commit.interval.ms", "1000");

        // custom config
        this.context.put(KafkaFlumeConstans.CUSTOME_TOPIC_KEY_NAME, "page_visit");
        this.context.put(KafkaFlumeConstans.DEFAULT_ENCODING, "UTF-8");
        this.context.put(KafkaFlumeConstans.CUSTOME_CONSUMER_THREAD_COUNT_KEY_NAME, "4");

        Configurables.configure(this.channel, this.context);

        rcs.setChannels(Lists.newArrayList(this.channel));

        this.source = new KafkaSource();
        this.source.setChannelProcessor(new ChannelProcessor(rcs));
        Configurables.configure(this.source, this.context);

    }

    /**
     * Test lifecycle.
     *
     * @throws InterruptedException              the interrupted exception
     * @throws InterruptedException              the interrupted exception
     */
    @Test
    public void testLifecycle() throws InterruptedException, LifecycleException {
        source.start();

        Thread.sleep(200000);

        source.stop();
    }


    /**
     * Test append.
     *
     * @throws Exception              the exception
     */
    public void testAppend() throws Exception {
        LOGGER.info("Begin Seding message to Kafka................");
        source.start();

        //send message
        sendMessageToKafka();

        Thread.sleep(5000);

        //get message from channel
        Transaction transaction = channel.getTransaction();

        try{

            transaction.begin();
            Event event;
            while ((event = channel.take()) != null) {
                LOGGER.info("#get channel########" + new String(event.getBody(), "UTF-8"));
                System.out.println("#get channel########" + new String(event.getBody(), "UTF-8"));
            }
            transaction.commit();
        } finally{
            transaction.close();
        }
    }

    /**
     * Send message to kafka.
     */
    private void sendMessageToKafka() {

        Properties parameters = new Properties();

        parameters.put("metadata.broker.list", "192.168.206.41:9092,192.168.206.41:9093,192.168.206.41:9094");
        parameters.put("serializer.class", "kafka.serializer.StringEncoder");
        parameters.put("partitioner.class", "com.huayu.metis.flume.source.SinglePartition");
        parameters.put("request.required.acks", "1");
        parameters.put("max.message.size", "1000000");
        parameters.put("producer.type", "sync");



        ProducerConfig config = new ProducerConfig(parameters);
        Producer producer = new Producer<String, String>(config);
        String encoding = KafkaFlumeConstans.DEFAULT_ENCODING;
        String topic = "error";
        KeyedMessage<String, String> data;

        String message = "writeDate,appId,logDate,kkkk,clientId\r\n2014-04-22 11:13:43,19,2014-04-22 11:13:43,aaaaa,3333";

        for (int i=0; i< 10; i++) {
            data = new KeyedMessage<String, String>(topic, message);
            producer.send(data);
        }
    }
}
