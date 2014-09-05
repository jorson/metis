package com.metis.monitor.syslog.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import com.huayu.meits.storm.kafka.*;
import com.metis.monitor.syslog.bolt.BatchingBolt;
import com.metis.monitor.syslog.bolt.OriginalParseBolt;
import com.metis.monitor.syslog.bolt.TransportBolt;
import com.metis.monitor.syslog.config.SysLogConfig;
import com.metis.monitor.syslog.util.ConstVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by Administrator on 14-8-7.
 */
public class KafkaSpoutTestTopology {

    private final BrokerHosts brokerHosts;
    private static Config config;

    public KafkaSpoutTestTopology(String kafkaZookeeper) throws Exception {
        brokerHosts = new ZkHosts(kafkaZookeeper);
        String configPath = "D:\\Documents\\metis_github\\ServerComponent\\SysLogMonitor\\src\\main\\resources\\monitor.syslog.properties";
        config = SysLogConfig.getInstance().loadConfig(configPath);
    }

    public StormTopology buildTopology() {
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "sys_log", "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.maxOffsetBehind = 0;
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("str-sys-log", new KafkaSpout(kafkaConfig), 1);
        //接收来至队列的数据(String)类型, 并将其转换为可流动的对象
        builder.setBolt("original-sys-log", new OriginalParseBolt(), 1)
                .shuffleGrouping("str-sys-log");
        //接收原始日志对象, 并转换为可统计对象, 根据APPID进行分组
        builder.setBolt("trans-sys-log", new TransportBolt(), 5)
                .fieldsGrouping("original-sys-log",
                        new Fields(ConstVariables.SYS_LOG_ORIGINAL_PARTITION_FIELD));
        //批量处理数据, 根据AppId进行分组处理
        builder.setBolt("batch-sys-log", new BatchingBolt(), 5)
                .fieldsGrouping("trans-sys-log",
                        new Fields(ConstVariables.SYS_LOG_ORIGINAL_PARTITION_FIELD));
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {

        //String kafkaZk = "ip-121207-240190.tianyu.nd:2181,ip-121207-240190.tianyu.nd:2182,ip-121207-240190.tianyu.nd:2183";
        String kafkaZk = "192168-205213:2181,192168-205213:2182,192168-205213:2183";
        KafkaSpoutTestTopology kafkaSpoutTestTopology = new KafkaSpoutTestTopology(kafkaZk);
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

        StormTopology stormTopology = kafkaSpoutTestTopology.buildTopology();
        if (args != null && args.length > 1) {
            String name = args[1];
            String dockerIp = args[2];
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(5);
            config.put(Config.NIMBUS_HOST, dockerIp);
            config.put(Config.NIMBUS_THRIFT_PORT, 6627);
            config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
            config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(dockerIp));
            StormSubmitter.submitTopology(name, config, stormTopology);
        } else {
            config.setNumWorkers(1);
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka", config, stormTopology);
        }
    }

    /*public static final Logger logger = LoggerFactory.getLogger(KafkaSportTopology.class);
    private final BrokerHosts brokerHosts;
    private static Config config;

    static {
        String configPath = "E:\\CodeInGit\\metis_github\\ServerComponent\\SysLogMonitor\\src\\main\\resources\\monitor.syslog.properties";
        try {
            config = SysLogConfig.getInstance().loadConfig(configPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public KafkaSportTopology() {
        String brokerList = SysLogConfig.getInstance().tryGet(SysLogConfig.ZOOKEEPER_HOSTS);
        brokerHosts = new ZkHosts(brokerList);
    }

    public static void main(String[] args) throws Exception {
        //设置每1秒发出一次
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 1000);
        KafkaSportTopology sportTopology = new KafkaSportTopology();
        StormTopology topology = sportTopology.buildTopology();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("sys_log", config, topology);

        Utils.sleep(2000000000);
        cluster.shutdown();
    }

    public StormTopology buildTopology() {
        String topic = SysLogConfig.getInstance().tryGet(SysLogConfig.KAFKA_TOPIC);
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topic, "/kafka_storm", "syslog");
        kafkaConfig.zkServers = new ArrayList<String>(){{
            add("192168-072166");
        }};
        kafkaConfig.zkPort = 2181;
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("syslog", new KafkaSpout(kafkaConfig), 1);
        builder.setBolt("print", new PrinterBolt()).shuffleGrouping("syslog");
        return builder.createTopology();
    }

    public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            logger.info(tuple.toString());
        }

    }*/
}
