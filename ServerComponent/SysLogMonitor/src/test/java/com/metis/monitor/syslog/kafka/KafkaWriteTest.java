package com.metis.monitor.syslog.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Administrator on 14-8-7.
 */
public class KafkaWriteTest {

    private static final int[] appId = new int[] {7, 19, 20, 35, 17};
    private static final int[] logLevel = new int[] {1, 2, 3, 4, 5};
    private static final String[] logMessage = new String[]
            {
                    "LogMessage1",
                    "LogMessage2",
                    "LogMessage3",
                    "LogMessage4",
                    "LogMessage5"
            };
    private static final String[] logCallStack = new String[]
            {
                    "LogCallStack1",
                    "LogCallStack2",
                    "LogCallStack3",
                    "LogCallStack4",
                    "LogCallStack5"
            };
    private static Random random = new Random();
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Test
    public void writeToKafka() throws InterruptedException {
        long events = 300;
        Properties props = new Properties();
        props.put("metadata.broker.list", "192168-072166:9091,192168-072166:9092,192168-072166:9093");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "com.metis.monitor.syslog.kafka.SimplePartitioner");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        List<KeyedMessage<String, String>> dataList = new ArrayList<KeyedMessage<String, String>>();

        for(long event = 0; event < events; event++){
            String message = buildOriginalSysLog();
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("sys_log", message);
            producer.send(data);
            //dataList.add(data);
            //每发送一笔记录停止一下
            Thread.sleep(500);
            System.out.println("Write Event:" + event);
        }
        //producer.send(dataList);
        System.out.println("Write Over!");
        producer.close();

/*        long events = 10;
        Random rnd = new Random();

        Properties props = new Properties();
        props.put("metadata.broker.list", "192168-072166:9091,192168-072166:9092,192168-072166:9093");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("partitioner.class", "com.metis.monitor.syslog.kafka.SimplePartitioner");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);
        List<KeyedMessage<String, String>> dataList = new ArrayList<KeyedMessage<String, String>>();

        for(long event = 0; event < events; event++){
            long runtime = new Date().getTime();
            String ip = "192.168.206." + rnd.nextInt(255);
            String msg = runtime + ", this is demo #" + ip;
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("tp_test", ip, msg);
            dataList.add(data);
        }
        producer.send(dataList);

        System.out.println("Write Over!" + Thread.currentThread().getId());
        producer.close();*/
    }

    private String buildOriginalSysLog() {
        int rndNum = random.nextInt(5);
        String result = String.format("%d\t%d\t%s\t%s\t%s",
                appId[rndNum],
                logLevel[rndNum],
                logMessage[rndNum],
                logCallStack[rndNum],
                dateFormat.format(new Date()));
        return result;
    }
}
