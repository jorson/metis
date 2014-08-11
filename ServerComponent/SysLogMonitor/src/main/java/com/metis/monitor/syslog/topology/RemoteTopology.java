package com.metis.monitor.syslog.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.huayu.meits.storm.kafka.*;
import com.metis.monitor.syslog.bolt.BatchingBolt;
import com.metis.monitor.syslog.bolt.OriginalParseBolt;
import com.metis.monitor.syslog.bolt.TransportBolt;
import com.metis.monitor.syslog.config.SysLogConfig;
import com.metis.monitor.syslog.util.ConstVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by Administrator on 14-8-8.
 */
public class RemoteTopology {

    private final BrokerHosts brokerHosts;
    private static final Logger logger = LoggerFactory.getLogger(RemoteTopology.class);
    private static Config topologyConfig;

    public RemoteTopology(String configPath) throws Exception {
        File file = new File(configPath);
        if(!file.exists()) {
            if(logger.isErrorEnabled()) {
                logger.error("RemoteTopology", "config file not exists!" + configPath);
            }
        }
        topologyConfig = SysLogConfig.getInstance().loadConfig(configPath);
        logger.info("RemoteTopology", "config path:" + configPath);
        System.out.println("config path:" + configPath);
        String kafkaZookeeper = SysLogConfig.getInstance().tryGet(SysLogConfig.ZOOKEEPER_HOSTS);
        brokerHosts = new ZkHosts(kafkaZookeeper);
    }

    public StormTopology buildTopology() {
        //读取Topic的名称
        String topic = SysLogConfig.getInstance().tryGet(SysLogConfig.KAFKA_TOPIC, "sys_log");
        //设置Spout的Config
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topic, "", "syslog");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("str-sys-log", new KafkaSpout(kafkaConfig), 1);
        //接收来至队列的数据(String)类型, 并将其转换为可流动的对象
        builder.setBolt("original-sys-log", new OriginalParseBolt(), 3)
                .shuffleGrouping("str-sys-log");
        //接收原始日志对象, 并转换为可统计对象, 根据APPID进行分组
        builder.setBolt("trans-sys-log", new TransportBolt(), 3)
                .fieldsGrouping("original-sys-log",
                        new Fields(ConstVariables.SYS_LOG_ORIGINAL_PARTITION_FIELD));
        //批量处理数据, 根据AppId进行分组处理
        builder.setBolt("batch-sys-log", new BatchingBolt(), 3)
                .fieldsGrouping("trans-sys-log",
                        new Fields(ConstVariables.SYS_LOG_ORIGINAL_PARTITION_FIELD));
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {

        if(args.length != 1) {
            if(logger.isErrorEnabled()) {
                logger.error("RemoteTopology", "SysLog Topology need 1 Args, First is config file PATH");
            }
            return;
        }
        RemoteTopology remoteTopology = new RemoteTopology(args[0]);
        //每秒的发出一次
        topologyConfig.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 1000);
        StormTopology stormTopology = remoteTopology.buildTopology();
        //提交处理
        StormSubmitter.submitTopology("system-log-monitor", topologyConfig, stormTopology);
        System.out.println("submit error");
    }
}
