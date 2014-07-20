package com.huayu.metis.storm.bolt.monitor;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.RotatingMap;
import clojure.lang.Obj;
import com.google.common.collect.Lists;
import com.huayu.metis.storm.ConstansValues;
import com.huayu.metis.storm.bolt.mysql.MySQLBoltBase;
import com.huayu.metis.storm.config.ApiMonitorConfig;

import java.util.*;

/**
 * Created by Administrator on 14-5-4.
 */
public class ApiMonitorMergeBolt extends MySQLBoltBase {

    Fields keyFields;
    Fields outFields;
    Map<String, GlobalStreamId> fieldLocations;
    RotatingMap<List<Object>, Map<GlobalStreamId, Tuple>> pending;
    int sourceNum;


    public ApiMonitorMergeBolt(Fields outFields) {
        this.outFields = outFields;

        //TODO:这里用硬编码固定了数据表的字段名称, 之后要考虑通过配置的方式
        this.columns = new String[] {"StopTimestamp","ApiUrl","SiteId","AppId","ClientId","StatusCode",
                "IpAddress","CallTimes","AvgResponseTime","SumRequestSize","SumResponseSize"};
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        //这里从配置中获取MySQL的配置
        this.url = map.get(ApiMonitorConfig.TARGET_URL).toString();
        this.userName = map.get(ApiMonitorConfig.TARGET_AUTH_USER).toString();
        this.password = map.get(ApiMonitorConfig.TARGET_AUTH_PASSWORD).toString();
        this.tableName = map.get(ApiMonitorConfig.TARGET_NAME).toString();

        //执行父类的准备文件
        super.prepare(map, topologyContext, outputCollector);

        //自己的准备过程
        this.fieldLocations = new HashMap<String, GlobalStreamId>();
        this.pending = new RotatingMap<List<Object>, Map<GlobalStreamId, Tuple>>(5, new ExpireCallback());
        sourceNum = this.context.getThisSources().size();
        Set<String> idFields = null;
        //获取所有数据源中的公共部分KeySet,将其设置保存为KeyFields, 同时将所有的OutFields都记录到FieldLocations中
        for(GlobalStreamId source : this.context.getThisSources().keySet()) {
            Fields fields = this.context.getComponentOutputFields(source.get_componentId(), source.get_streamId());
            Set<String> setFields = new HashSet<String>(fields.toList());
            if(idFields == null){
                idFields = setFields;
            } else {
                idFields.retainAll(setFields);
            }
            //将所有的输出Field都放到fieldLocations中, 以便于获取
            for (String outField : this.outFields) {
                for(String srcField : fields) {
                    if(outField.equals(srcField)){
                        fieldLocations.put(outField, source);
                    }
                }
            }
        }

        this.keyFields = new Fields(new ArrayList<String>(idFields));
        if (this.fieldLocations.size() != this.outFields.size()) {
            throw new RuntimeException("Cannot find all outfields among sources");
        }
    }

    @Override
    public void execute(Tuple tuple) {
        List<Object> key = tuple.select(this.keyFields);
        GlobalStreamId streamId = new GlobalStreamId(tuple.getSourceComponent(), tuple.getSourceStreamId());
        if(!this.pending.containsKey(key)) {
            pending.put(key, new HashMap<GlobalStreamId, Tuple>());
        }
        Map<GlobalStreamId, Tuple> parts = pending.get(key);
        if (parts.containsKey(streamId))
            throw new RuntimeException("Received same side of single join twice");
        parts.put(streamId, tuple);
        if(parts.size() == sourceNum) {
            this.pending.remove(key);
            List<Object> joinResult = new ArrayList<Object>();
            for(String outField : this.outFields) {
                GlobalStreamId loc = fieldLocations.get(outField);
                joinResult.add(parts.get(loc).getValueByField(outField));
            }

            StringBuilder output = new StringBuilder();
            for(Object k : key) {
                output.append(k);
                output.append(",");
            }
            for(Object v : joinResult) {
                output.append(v);
                output.append(",");
            }

            //DEBUG MODE
            //if(conf.get(ApiMonitorConfig.DEBUG_MODE).toString().equalsIgnoreCase("false")){
                //TODO: 这里写入到MySQL数据库中, 将数据直接PrintLn
            //    System.out.println("[Join Result]" + output.toString());
            //}
            //RELEASE MODE
            //else {
                System.out.println("[Join Result]" + output.toString());
                List<Object> values = Lists.newArrayList((Object[])(output.toString().split(",")));
                super.put(values);
            //}
            //应答
            for(Tuple part : parts.values()) {
                this.collector.ack(part);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(this.outFields);
    }

    @Override
    public void cleanup() {

    }

    private class ExpireCallback implements RotatingMap.ExpiredCallback<List<Object>, Map<GlobalStreamId, Tuple>> {

        @Override
        public void expire(List<Object> o, Map<GlobalStreamId, Tuple> tupleMap) {
            for(Tuple tuple : tupleMap.values()) {
                collector.fail(tuple);
            }
        }
    }
}
