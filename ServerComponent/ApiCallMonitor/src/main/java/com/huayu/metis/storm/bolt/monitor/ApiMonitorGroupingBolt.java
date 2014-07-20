package com.huayu.metis.storm.bolt.monitor;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.huayu.metis.storm.ConstansValues;
import com.huayu.metis.storm.spout.ApiCallDocument;
import com.rits.cloning.Cloner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bolt接收基于时间间隔的Spout传递过来的数据数组, 根据特定业务规则进行分组, 并将分组后的数据发送出去
 * Created by Administrator on 14-4-29.
 */
public class ApiMonitorGroupingBolt extends BaseRichBolt {
    public Logger logger = LoggerFactory.getLogger(ApiMonitorGroupingBolt.class);
    private Map<Object, List<Map<String, Object>>> groupingMap = new ConcurrentHashMap<Object, List<Map<String, Object>>>();
    private OutputCollector collector;
    private Cloner cloner;
    /**
     * Bolt接收基于时间间隔的Spout传递过来的数据数组, 根据特定业务规则进行分组
     */
    public ApiMonitorGroupingBolt() {
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.cloner = new Cloner();
    }

    @Override
    public void execute(Tuple tuple) {
        //获取tuple传递过来的消息, 这是一个按照固定时间间隔分组的对象, 外围还包裹Object
        //获取分组时间键
        long groupTimestamp = tuple.getLongByField(ConstansValues.API_MONITOR_TIME_BASE_GROUP_KEY);
        //获取分组数据
        Object orignalObj = tuple.getValueByField(ConstansValues.API_MONITOR_TIME_BASE_OUTPUT_KEY);
        List<ApiCallDocument> realDocList = null;
        if(orignalObj != null){
            realDocList = (List<ApiCallDocument>)orignalObj;
        }
        logger.debug("[hy]deal with " + realDocList.size() + " records");

        if(realDocList == null) {
            collector.fail(tuple);
            return;
        }

        //循环Tuple中的数据
        //for(ApiCallDocument document : realDocList) {
        for(int i=0; i<realDocList.size(); i++) {
            ApiCallDocument document = realDocList.get(i);
            Object key = document.getKey();
            if(!groupingMap.containsKey(key)){
                List<Map<String, Object>> values = new ArrayList<Map<String, Object>>();
                values.add(document.getValue());
                groupingMap.put(key, values);
            } else {
                groupingMap.get(key).add(document.getValue());
            }
        }


        logger.debug("[hy]grouping " + groupingMap.size() + " groups");
        Map<Object, List<Map<String, Object>>> sendGrouping = cloner.deepClone(groupingMap);
        //将分组后的数据发出
        for(Map.Entry<Object,  List<Map<String, Object>>> entry : sendGrouping.entrySet()) {
            collector.emit(new Values(groupTimestamp, entry.getKey(), entry.getValue()));
        }
        //清空分组
        groupingMap.clear();
        //确认应答
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(
                ConstansValues.API_MONITOR_GROUPING_TIMESTAMP_FILED,
                ConstansValues.API_MONITOR_GROUPING_KEY_FILED,
                ConstansValues.API_MONITOR_GROUPING_VALUE_FILED));
    }
}
