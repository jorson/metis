package com.huayu.metis.storm.bolt.monitor;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import clojure.lang.Obj;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.huayu.metis.storm.ConstansValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 计算API接口的总响应字节的Bolt
 * Created by Administrator on 14-5-4.
 */
public class ApiMonitorSumBolt extends BaseRichBolt {

    private OutputCollector collector;
    private static final Logger logger = LoggerFactory.getLogger(ApiMonitorSumBolt.class);
    private String[] sumFields;
    private int fieldCount = 0;

    public ApiMonitorSumBolt(String[] sumFields) {
        if(sumFields == null || sumFields.length == 0) {
            throw new IllegalArgumentException("must declare one or more filed(s) for sum");
        }
        this.sumFields = sumFields;
        this.fieldCount = sumFields.length;
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
        Long[] sumResult = new Long[fieldCount];
        int tmpInt = 0;

        //如果获取到的值对象不为空
        if(objValue != null) {
            List<Map<String, Object>> realValue = (List<Map<String, Object>>)objValue;

            for(Map<String, Object> value : realValue) {
                for(int i=0; i<fieldCount; i++) {
                    if(value.containsKey(this.sumFields[i])){
                        tmpInt = Integer.valueOf(value.get(this.sumFields[i]).toString());
                    } else {
                        tmpInt = 0;
                    }
                    if(sumResult[i] == null) {
                        sumResult[i] = new Long(0);
                    }
                    sumResult[i] += tmpInt;
                }
            }
        }
        String output = Joiner.on(",").join(sumResult);
        //发出结果
        collector.emit(new Values(realKey, output));
        if(logger.isDebugEnabled()) {
            logger.debug(realKey.toString() + "," + output);
        }
        System.out.println(realKey.toString() + "," + output);
        //响应
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(
                ConstansValues.API_MONITOR_MERGE_KEY,
                ConstansValues.API_MONITOR_MERGE_SUM));
    }
}
