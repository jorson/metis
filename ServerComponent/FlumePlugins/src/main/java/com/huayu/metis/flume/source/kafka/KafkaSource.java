package com.huayu.metis.flume.source.kafka;

import com.google.common.collect.ImmutableMap;
import com.huayu.metis.flume.utility.CsvUtils;
import com.huayu.metis.flume.utility.KafkaFlumeConstans;
import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.nio.charset.Charset;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Administrator on 14-4-4.
 */
public class KafkaSource extends AbstractSource implements Configurable, EventDrivenSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);
    private final Charset utf8Code = Charset.forName(KafkaFlumeConstans.DEFAULT_ENCODING);
    private Properties parameters;
    private Context context;
    private ConsumerConnector consumerConnector;
    private ExecutorService executorService;
    private SourceCounter sourceCounter;

    public static final String DEFAULT_DATEFORMAT = "yyyy-MM-dd HH:mm:ss";

    @Override
    public void configure(Context context) {
        this.context = context;
        ImmutableMap<String, String> props = context.getParameters();

        this.parameters = new Properties();
        for (String key : props.keySet()) {
            String value = props.get(key);
            this.parameters.put(key, value);
        }

        //source monitoring count
        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
    }

    @Override
    public synchronized void start() {
        super.start();
        //启动Source计数器
        sourceCounter.start();
        LOGGER.info("[hy]Kafka Source started...");
        LOGGER.info("[hy]Consumer Config..{}", this.parameters);
        //读取配置文件
        ConsumerConfig consumerConfig = new ConsumerConfig(this.parameters);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        String threadCount = (String) this.parameters.get(KafkaFlumeConstans.CUSTOME_CONSUMER_THREAD_COUNT_KEY_NAME);
        Integer threadNum = new Integer(threadCount);
        //根据配置, 得到不同的获取策略
        String topicInfo = null;
        //获取特定的Topic
        if(this.parameters.containsKey(KafkaFlumeConstans.CUSTOME_TOPIC_KEY_NAME)){
            topicInfo = this.parameters.getProperty(KafkaFlumeConstans.CUSTOME_TOPIC_KEY_NAME);
            processSpecifiesTopic(topicInfo, threadNum);
        }
        //使用TopicFilter获取
        else if(this.parameters.containsKey(KafkaFlumeConstans.CUSTOME_TOPIC_FILTER)){
            topicInfo = this.parameters.getProperty(KafkaFlumeConstans.CUSTOME_TOPIC_FILTER);
            processIndeterminacyTopic(topicInfo, threadNum);
        } else{
            throw new IllegalArgumentException("Kafka Topic is empty");
        }
    }

    private void processSpecifiesTopic(String topicInfo, int threadNum){
        String[] topics = topicInfo.split(",");
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        for(String t : topics){
            topicCountMap.put(t, threadNum);
        }
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap
                = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = mergeTopicStream(consumerMap);
        //launch all the thread
        this.executorService = Executors.newFixedThreadPool(threadNum);
        //创建消息的消费者
        int tNumber = 0;
        for(final KafkaStream stream : streams){
            this.executorService.submit(new ConsumerRunner(stream, tNumber, sourceCounter));
            tNumber++;
        }
    }

    private void processIndeterminacyTopic(String topicRegx, int threadNum){
        TopicFilter filter = new Whitelist(topicRegx);

        List<KafkaStream<byte[], byte[]>> streams =
                consumerConnector.createMessageStreamsByFilter(filter);
        //launch all the thread
        this.executorService = Executors.newFixedThreadPool(threadNum);
        //创建消息的消费者
        int tNumber = 0;
        for(final KafkaStream stream : streams){
            this.executorService.submit(new ConsumerRunner(stream, tNumber, sourceCounter));
            tNumber++;
        }
    }

    private List<KafkaStream<byte[], byte[]>> mergeTopicStream(Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap){
        List<KafkaStream<byte[], byte[]>> mergeConsumer = new ArrayList<KafkaStream<byte[], byte[]>>();
        for(Map.Entry<String, List<KafkaStream<byte[], byte[]>>> entry : consumerMap.entrySet()){
            mergeConsumer.addAll(entry.getValue());
        }
        return mergeConsumer;
    }

    @Override
    public synchronized void stop() {
        try{
            shutdown();
        } catch (Exception ex){
            ex.printStackTrace();
        }
        super.stop();
        sourceCounter.stop();

        ObjectName objName = null;
        try{
            objName = new ObjectName("org.apache.flume.source :type=" + getName());
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(objName);
        } catch (Exception ex) {
            System.out.println("Failed to unregister the monitored counter: "
                    + objName + ex.getMessage());
        }
    }

    private void shutdown() throws Exception{
        if(consumerConnector != null){
            consumerConnector.commitOffsets();
            consumerConnector.shutdown();
        }
        if(executorService != null){
            executorService.shutdown();
        }
        executorService.awaitTermination(300, TimeUnit.SECONDS);
    }

    public static Date parse(String text, String pattern) {
        if (StringUtils.isEmpty(pattern)) {
            pattern = DEFAULT_DATEFORMAT;
        }
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(pattern);
            return sdf.parse(text);
        } catch (Exception ex) {
            //这里做Error的记录
            LOGGER.error("Date parse happen error![" + text + "," + pattern + "]");
            return null;
            //throw new IllegalArgumentException("Date parse happen error![" + text + "," + pattern + "]");
        }
    }

    private class ConsumerRunner implements Runnable{

        private KafkaStream kafkaStream;
        private int threadNumber;
        private SourceCounter sourceCounter;

        public ConsumerRunner(KafkaStream stream, int threadNum, SourceCounter counter){
            this.kafkaStream = stream;
            this.threadNumber = threadNum;
            this.sourceCounter = counter;
        }

        @Override
        public void run() {
            ConsumerIterator<byte[], byte[]> it = this.kafkaStream.iterator();
            try{
                Map<String, String> headers = new HashMap<String, String>();
                String topic, realTopic, message, realMessage;
                while(it.hasNext()){
                    MessageAndMetadata<byte[], byte[]> messageAndMeta = it.next();
                    //获取消息中的Topic
                    topic = messageAndMeta.topic();
                    LOGGER.debug("[hy]Event Topic is " + topic);
                    //获取真实的Topic
                    realTopic = getRealTopic(topic);
                    //将Topic放到Header中
                    headers.put(KafkaFlumeConstans.HEADER_TOPIC_KEY, realTopic);
                    //使用特定的编码生成Message
                    message = new String(messageAndMeta.message(), utf8Code);

                    if(message == null)
                        continue;

                    List<String[]> recMessage = CsvUtils.splitMultiLines(message);
                    Event event = null;
                    if(recMessage.size() == 2) {
                        event = csvFormatHandle(topic, headers, recMessage);
                    } else {
                        event = normalFormatHandle(headers, message);
                    }

                    if(event == null)
                        continue;

                    //获取通道, 将事件推入通道
                    getChannelProcessor().processEvent(event);

                    //更新计数器
                    this.sourceCounter.incrementEventAcceptedCount();
                }

            } catch (Exception ex){
                ex.printStackTrace();
            }
        }

        private Event csvFormatHandle(String topic, Map<String, String> headers, List<String[]> recMessage) {
            String realMessage = "";
            //消息体的第3分量是日期
            Date logDate = parse(recMessage.get(1)[2], null);
            //如果转换失败...继续处理下一条数据
            if(logDate == null) {
                return null;
            }
            long seconds = logDate.getTime();
            //消息提的第2分量是AppId
            String appid = recMessage.get(1)[1];
            //日志写入日期
            headers.put(KafkaFlumeConstans.HEADER_TIMESTAMP_KEY, String.valueOf(seconds));
            //写入appid
            headers.put(KafkaFlumeConstans.HEADER_APP_ID_KEY, appid);
            //如果消息的Topic中包含有"monitor", 表示是API监控所需要的消息
            if (topic.startsWith("monitor")) {
                //将消息中的第一行Join后写入Header
                String messageField = CsvUtils.join(recMessage.get(0));
                headers.put(KafkaFlumeConstans.HEADER_FIELDS_KEY, messageField);
            }
            //将第2行的数据拼接后写入Event的Body中
            realMessage = CsvUtils.join(recMessage.get(1));

            //记录DEBUG日志
            System.out.println("[hy]Receive Message [Thread " + this.threadNumber
                    + ", Topic:" + topic + "AppId:"
                    + appid + ", Message:" + recMessage.get(1) + "]");
            //创建事件
            Event event = EventBuilder.withBody(realMessage, utf8Code, headers);
            return event;
        }

        private Event normalFormatHandle(Map<String, String> headers, String message) {
            if("".equals(message)){
                return null;
            }
            if(message.indexOf("\t") == -1) {
                return null;
            }
            System.out.println("[hy]Receive Message" + message);
            Event event = EventBuilder.withBody(message, utf8Code, headers);
            return event;
        }

        private String getRealTopic(String topic) {
            if(topic.indexOf(".") == -1) {
                return topic;
            }

            int lastPoint = topic.lastIndexOf(".");
            String realTopic = topic.substring(lastPoint+1);
            LOGGER.debug("[hy]lastPoint:" + lastPoint + ",realTopic:" + realTopic);
            //获取Topic分片的最后一个,作为Topic
            return realTopic;
        }
    }
}
