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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 计算API接口的平均响应时间的Bolt
 * Created by Administrator on 14-5-4.
 */
public class ApiMonitorAvgBolt extends BaseRichBolt {

    private OutputCollector collector;
    private static final Logger logger = LoggerFactory.getLogger(ApiMonitorAvgBolt.class);
    private String[] dealFields;
    private int fieldCount = 0;

    public ApiMonitorAvgBolt(String[] dealFields) {
        if(dealFields == null || dealFields.length == 0) {
            throw new IllegalArgumentException("must declare one or more filed(s) for sum");
        }
        this.dealFields = dealFields;
        this.fieldCount = dealFields.length;
    }

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
        Integer[] avgCounter = new Integer[this.fieldCount];
        Double[] tmpSum = new Double[this.fieldCount];
        Double[] avgResult = new Double[this.fieldCount];

        //如果获取到的值对象不为空
        if(objValue != null) {
            List<Map<String, Object>> realValue = (List<Map<String, Object>>)objValue;
            for(Map<String, Object> value : realValue) {
                for(int i=0; i<fieldCount; i++) {
                    if(value.containsKey(this.dealFields[i])){
                        if(tmpSum[i] == null) {
                            tmpSum[i] = new Double(0.0d);
                        }
                        tmpSum[i] +=  Integer.valueOf(value.get(this.dealFields[i]).toString());
                        if(avgCounter[i] == null) {
                            avgCounter[i] = new Integer(0);
                        }
                        avgCounter[i] += 1;
                    }
                }
            }
        }

        for(int i=0; i<fieldCount; i++) {
            if(avgCounter[i] == 0) {
                avgResult[i] = 0.0d;
            } else {
                avgResult[i] = tmpSum[i] / avgCounter[i];
                avgResult[i]= ((int)(avgResult[i].doubleValue() * 100))/100.0;
            }
        }

        String output = Joiner.on(",").join(avgResult);

        if(logger.isDebugEnabled()) {
            logger.debug(objKey.toString() + "," + output);
        }
        System.out.println(realKey.toString() + "," + output);
        collector.emit(new Values(realKey, output));
        //响应
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(
                ConstansValues.API_MONITOR_MERGE_KEY,
                ConstansValues.API_MONITOR_MERGE_AVG));
    }
}
