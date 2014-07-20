package com.huayu.metis.flume.source;

import kafka.admin.AdminUtils;
import kafka.admin.TopicCommand;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.hadoop.util.ZKUtil;
import org.junit.Test;
import scala.Option;
import scala.collection.Map;
import scala.collection.Seq;
import scala.collection.immutable.List;

import java.util.Iterator;
import java.util.Properties;

/**
 * Created by Administrator on 14-5-22.
 */
public class KafkaTopicTest {



    @Test
    public void createTopic() {

        String topic = "exception";

        ZkClient client = new ZkClient("192.168.206.41:2181,192.168.205.3:2181,192.168.205.9:2181", 30000, 30000, new Ada());
        boolean isExists = AdminUtils.topicExists(client, topic);
        System.out.println(isExists);

        if(!isExists) {
            AdminUtils.createTopic(client, topic, 3, 1, new Properties());
            //createTopic1();
        }
        isExists = AdminUtils.topicExists(client, topic);
        System.out.println(isExists);

        Seq<String> topics = ZkUtils.getAllTopics(client);
        if(topics.size() > 0) {
            scala.collection.Iterator<String> iterator = topics.toIterator();
            while (iterator.hasNext()) {
                System.out.println(iterator.next());
            }
        }
    }

    class Ada implements ZkSerializer {
        @Override
        public byte[] serialize(Object o) throws ZkMarshallingError {
            return ZKStringSerializer.serialize(o);

        }

        @Override
        public Object deserialize(byte[] bytes) throws ZkMarshallingError {
            return ZKStringSerializer.deserialize(bytes);
            //return null;
        }
    }

    private void createTopic1() {
        String [] arguments = new String[8];
        arguments[0] = "--zookeeper";
        arguments[1] = "192.168.206.41:2181,192.168.205.3:2181,192.168.205.9:2181";
        arguments[2] = "--replica";
        arguments[3] = "1";
        arguments[4] = "--partition";
        arguments[5] = "1";
        arguments[6] = "--topic";
        arguments[7] = "test.topic1";

        TopicCommand.main(arguments);
    }
}
