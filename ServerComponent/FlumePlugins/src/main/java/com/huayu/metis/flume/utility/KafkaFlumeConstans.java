package com.huayu.metis.flume.utility;

/**
 * Created by Administrator on 14-4-4.
 */
public class KafkaFlumeConstans {

    public static final String HEADER_TOPIC_KEY = "topic";
    public static final String HEADER_TIMESTAMP_KEY = "timestamp";
    public static final String HEADER_FIELDS_KEY = "fields";
    public static final String HEADER_APP_ID_KEY = "appid";
    public static final String DEFAULT_TOPIC = "metis";
    public static final String DEFAULT_APP_ID = "0";
    /**
     * The constant PARTITION_KEY_NAME.
     */
    public static final String PARTITION_KEY_NAME = "custom.partition.key";
    /**
     * The constant ENCODING_KEY_NAME.
     */
    public static final String ENCODING_KEY_NAME = "custom.encoding";
    /**
     * The constant DEFAULT_ENCODING.
     */
    public static final String DEFAULT_ENCODING = "UTF-8";
    /**
     * 指定特定的Topic, 只有该Topic的消息会被接受
     */
    public static final String CUSTOME_TOPIC_KEY_NAME = "custom.topic.name";
    /**
     * 指定Topic的筛选器, 所有符合筛选器的Topic都会被接受
     */
    public static final String CUSTOME_TOPIC_FILTER = "custom.topic.filter";
    /**
     * The constant CUSTOME_TOPIC_KEY_NAME.
     */
    public static final String CUSTOME_CONSUMER_THREAD_COUNT_KEY_NAME = "custom.thread.per.consumer";
}
