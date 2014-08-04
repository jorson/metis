package com.metis.monitor.syslog.topology;

import backtype.storm.topology.TopologyBuilder;
import com.huayu.meits.storm.kafka.KafkaSpout;

/**
 * Created by Administrator on 14-8-4.
 */
public class LocalSysLogTopology {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        //设置基源数据
        builder.setSpout("kafka_source", new KafkaSpout(null), 10);
    }
}
