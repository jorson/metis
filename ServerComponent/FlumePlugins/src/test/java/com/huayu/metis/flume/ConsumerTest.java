package com.huayu.metis.flume;

import com.huayu.metis.flume.utility.CsvUtils;
import com.huayu.metis.flume.utility.KafkaFlumeConstans;
import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.junit.*;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Administrator on 14-5-9.
 */
public class ConsumerTest {

/*    private Properties parameters;
    private ConsumerConnector consumerConnector;
    private ExecutorService executorService;*/
    private final Charset utf8Code = Charset.forName(KafkaFlumeConstans.DEFAULT_ENCODING);

/*    @Before
    public void setup() {
        this.parameters = new Properties();

        this.parameters.put("zookeeper.connect", "192.168.206.41:2181,192.168.205.3:2181,192.168.205.9:2181");
        this.parameters.put("group.id", "testGroup");
        this.parameters.put("zookeeper.session.timeout.ms", "400");
        this.parameters.put("zookeeper.sync.time.ms", "200");
        this.parameters.put("auto.commit.interval.ms", "1000");
    }*/

    //@Test
    public void getRealTopic() {
        String topic = "test";
        if(topic.indexOf(".") == -1) {
            System.out.println("[hy]realTopic:"+topic);
        }

        int lastPoint = topic.lastIndexOf(".");
        String realTopic = topic.substring(lastPoint+1);
        System.out.println("[hy]realTopic:"+realTopic);
    }

/*    @Test
    public void ConsumerFilterTest() {
        ConsumerConfig consumerConfig = new ConsumerConfig(this.parameters);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        TopicFilter filter = new Whitelist("test.*");
        List<KafkaStream<byte[], byte[]>> streams =
                consumerConnector.createMessageStreamsByFilter(filter);
        this.executorService = Executors.newFixedThreadPool(4);
        //创建消息的消费者
        int tNumber = 0;
        for(final KafkaStream stream : streams){
            this.executorService.submit(new ConsumerRunner(stream, tNumber));
            tNumber++;
        }
    }*/

/*    @After
    public void cleanup() throws InterruptedException {
        if(executorService != null){
            executorService.shutdown();
        }
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }*/

    private class ConsumerRunner implements Runnable{

        private KafkaStream kafkaStream;
        private int threadNumber;

        public ConsumerRunner(KafkaStream stream, int threadNum){
            this.kafkaStream = stream;
            this.threadNumber = threadNum;
        }

        @Override
        public void run() {
            ConsumerIterator<byte[], byte[]> it = this.kafkaStream.iterator();
            try{
                String topic, message;
                while(it.hasNext()){
                    MessageAndMetadata<byte[], byte[]> messageAndMeta = it.next();
                    //获取消息中的Topic
                    topic = messageAndMeta.topic();
                    //将Topic放到Header中
                    //使用特定的编码生成Message
                    message = new String(messageAndMeta.message(), utf8Code);

                    if(message != null){
                        System.out.println("[hy]Receive Message [Thread " + this.threadNumber +
                                ", Topic:" + topic + ", Message:" + message + "]");
                    }
                }

            } catch (Exception ex){
                ex.printStackTrace();
            }
        }
    }
}
