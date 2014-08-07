package com.metis.monitor.syslog.topology;

import backtype.storm.topology.TopologyBuilder;
import com.huayu.meits.storm.kafka.BrokerHosts;
import com.huayu.meits.storm.kafka.KafkaSpout;
import com.huayu.meits.storm.kafka.ZkHosts;
import com.metis.monitor.syslog.config.SysLogConfig;

/**
 * Created by Administrator on 14-8-4.
 */
public class LocalSysLogTopology {

    public static void main(String[] args) throws Exception {
        String configPath = "E:\\CodeInGit\\metis_github\\ServerComponent\\SysLogMonitor\\src\\test\\resources\\monitor.syslog.properties";

        TopologyBuilder builder = new TopologyBuilder();
        //设置基源数据
        builder.setSpout("kafka_source", new KafkaSpout(null), 10);
    }
}
