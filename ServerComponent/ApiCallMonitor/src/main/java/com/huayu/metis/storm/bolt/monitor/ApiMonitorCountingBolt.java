package com.huayu.metis.storm.bolt.monitor;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.common.base.Joiner;
import com.huayu.metis.storm.ConstansValues;
import com.huayu.metis.storm.bolt.mysql.MySQLBoltBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * 对分组对象中的数量执行计数操作
 * Created by Administrator on 14-4-30.
 */
public class ApiMonitorCountingBolt extends BaseRichBolt {

    private OutputCollector collector;
    private static final Logger logger = LoggerFactory.getLogger(ApiMonitorCountingBolt.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        //获取分组的时间键
        Object objTimeStamp = tuple.getValueByField(ConstansValues.API_MONITOR_GROUPING_TIMESTAMP_FILED);
        //获取其他分组信息
        Object objKey = tuple.getValueByField(ConstansValues.API_MONITOR_GROUPING_KEY_FILED);
        //获取分组中的数据
        Object objValue = tuple.getValueByField(ConstansValues.API_MONITOR_GROUPING_VALUE_FILED);

        //真实的分组键
        String realKey = objTimeStamp + "," + objKey;

        int valueCount = 0;
        //如果获取到的值对象不为空
        if(objValue != null) {
            List<Map<String, Object>> realValue = (List<Map<String, Object>>)objValue;
            valueCount = realValue.size();
        }

        //发出计数结果
        collector.emit(new Values(realKey, valueCount));
        if(logger.isDebugEnabled()) {
            logger.debug(realKey + "," + valueCount);
        }
        System.out.println(realKey + "," + valueCount);
        //响应
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(
                ConstansValues.API_MONITOR_MERGE_KEY,
                ConstansValues.API_MONITOR_MERGE_COUNT));
    }

    @Override
    public void cleanup() {
    }
}
