package com.metis.monitor.syslog.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import com.huayu.meits.storm.kafka.*;
import com.metis.monitor.syslog.config.SysLogConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * Created by Administrator on 14-8-7.
 */
public class KafkaSportTopology {

    public static final Logger logger = LoggerFactory.getLogger(KafkaSportTopology.class);
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

    }
}
