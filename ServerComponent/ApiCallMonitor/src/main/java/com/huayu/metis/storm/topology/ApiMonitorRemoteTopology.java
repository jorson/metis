package com.huayu.metis.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.huayu.metis.storm.ConstansValues;
import com.huayu.metis.storm.bolt.monitor.*;
import com.huayu.metis.storm.config.ApiMonitorConfig;
import com.huayu.metis.storm.spout.ApiCallDocument;
import com.huayu.metis.storm.spout.mongo.TimeBaseMongoSpout;

/**
 * Created by Administrator on 14-5-7.
 */
public class ApiMonitorRemoteTopology {

    public static void main(String[] args) throws Exception {

        if(args.length != 1) {
            throw new RuntimeException("Api Monitor Topology need 1 Args, First is config file PATH");
        }
        System.out.println(args[0]);
        //获取组件的配置
        ApiMonitorConfig apiMonitorConfig = new ApiMonitorConfig(args[0]);
        Config config = apiMonitorConfig.build();

        TopologyBuilder builder = new TopologyBuilder();
        //设置基于源的数据
        builder.setSpout("source", new TimeBaseMongoSpout(ApiCallDocument.class));
        builder.setBolt("grouping", new ApiMonitorGroupingBolt()).shuffleGrouping("source");
        builder.setBolt("count", new ApiMonitorCountingBolt()).shuffleGrouping("grouping");
        builder.setBolt("sum", new ApiMonitorSumBolt(new String[]{"requestSize", "responseSize"})).shuffleGrouping("grouping");
        builder.setBolt("avg", new ApiMonitorAvgBolt(new String[]{"responseTime"})).shuffleGrouping("grouping");
        builder.setBolt("merge", new ApiMonitorMergeBolt(new Fields(
                ConstansValues.API_MONITOR_MERGE_COUNT,
                ConstansValues.API_MONITOR_MERGE_AVG,
                ConstansValues.API_MONITOR_MERGE_SUM)))
                .fieldsGrouping("count", new Fields(ConstansValues.API_MONITOR_MERGE_KEY))
                .fieldsGrouping("sum", new Fields(ConstansValues.API_MONITOR_MERGE_KEY))
                .fieldsGrouping("avg", new Fields(ConstansValues.API_MONITOR_MERGE_KEY));

        //这里是做测试用的
        StormSubmitter.submitTopology("api-monitor", config, builder.createTopology());
        System.out.println("Execute End");
    }
}
