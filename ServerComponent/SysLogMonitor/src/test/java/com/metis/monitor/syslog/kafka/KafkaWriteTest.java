package com.metis.monitor.syslog.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Administrator on 14-8-7.
 */
public class KafkaWriteTest {

    private static final int[] userId = new int[] {1000, 2000, 3000, 4000, 5000};
    private static final int[] appId = new int[] {7, 19, 20, 35, 17};
    private static final int[] logLevel = new int[] {1, 2, 3, 4, 5};
    private static final String[] logMessage = new String[]
            {
                    "中文消息1",
                    "中文消息2",
                    "中文消息3",
                    "中文消息4",
                    "中文消息5"
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
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    @Test
    public void writeVisitLogToKafka()  throws InterruptedException, UnsupportedEncodingException {
        long events = 3000;
        Properties props = new Properties();
        props.put("metadata.broker.list", "192168-205213:9091,192168-205213:9092,192168-205213:9093");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "com.metis.monitor.syslog.kafka.SimplePartitioner");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        List<KeyedMessage<String, String>> dataList = new ArrayList<KeyedMessage<String, String>>();

        for(long event = 0; event < events; event++){
            String message = buildPageVisit();
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
    }

    @Test
    public void writeToKafkaThread() throws InterruptedException, UnsupportedEncodingException {
        for(int i=0; i<2; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        long events = 1;
                        Properties props = new Properties();
                        props.put("metadata.broker.list", "192168-205213:9091,192168-205213:9092,192168-205213:9093");
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
                    } catch(Exception ex) {
                        ex.printStackTrace();
                    }
                }
            });
            thread.start();
        }
        Thread.sleep(20000000);
    }


    @Test
    public void writeToKafka() throws InterruptedException, UnsupportedEncodingException {
        long events = 300;
        Properties props = new Properties();
        props.put("metadata.broker.list", "192168-205213:9091,192168-205213:9092,192168-205213:9093");
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
    }

    private String buildOriginalSysLog() throws UnsupportedEncodingException {
        int rndNum = random.nextInt(5);
        String result = String.format("%d\t%d\t%s\t%s\t%s",
                appId[rndNum],
                logLevel[rndNum],
                logMessage[rndNum],
                logCallStack[rndNum],
                dateFormat.format(new Date()));
        return URLEncoder.encode(result, "UTF-8");
    }

    /**
     * 将原始输入字符串分解,并转换为实体
     * 原始字符串格式: [UcCode]\t[UserId]\t[AppId]\t[TerminalCode]\t[IpAddress]\t[ReferPage]\t[visitPage]\t[visitPageParam]\t[VisitTime]
     */

    private String buildPageVisit() throws UnsupportedEncodingException {
        int rndNum = random.nextInt(5);
        String result = String.format("auc\t%d\t%d\t%d\t35124567\t%s\t%s\t%s\t%s",
                userId[rndNum],
                appId[rndNum],
                logLevel[rndNum],
                logMessage[rndNum],
                logCallStack[rndNum],
                logMessage[rndNum],
                dateFormat.format(new Date()));
        return result;
    }
}
