package com.metis.monitor.syslog.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import com.huayu.meits.storm.kafka.KafkaSpout;
import com.huayu.meits.storm.kafka.SpoutConfig;

import java.util.Map;

/**
 * 基于时间线的KafkaSpout
 * Created by Administrator on 14-8-4.
 */
public class TimeBaseKafkaSpout extends KafkaSpout {

    public TimeBaseKafkaSpout(SpoutConfig spoutConf) {
        super(spoutConf);
    }
}
