package com.metis.monitor.syslog.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.metis.monitor.syslog.entry.OriginalSysLog;
import com.metis.monitor.syslog.entry.SysLogDetail;
import com.metis.monitor.syslog.entry.SysLogType;
import com.metis.monitor.syslog.util.ConstVariables;
import com.metis.monitor.syslog.util.SysLogTypeManager;
import com.metis.monitor.syslog.util.SysLogTypeMissing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 用于将原始数据转换为后续正式数据
 * Created by Administrator on 14-8-6.
 */
public class TransportBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(TransportBolt.class);
    private static final long serialVersionUID = 1L;

    private OutputCollector collector;
    private SysLogTypeManager logTypeManager;

    public TransportBolt() {

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext,
                        OutputCollector collector) {
        this.collector = collector;
        //构建LogTypeManager的单例
        SysLogTypeMissing missing = new SysLogTypeMissing();
        this.logTypeManager = SysLogTypeManager.getInstance(missing);
    }

    @Override
    public void execute(Tuple tuple) {
        //获取来自KafkaSpout的原始数据
        //获取原始日志对象
        Integer partitionKey = tuple.getIntegerByField(ConstVariables.SYS_LOG_ORIGINAL_PARTITION_FIELD);
        Object originalObj = tuple.getValueByField(ConstVariables.SYS_LOG_ORIGINAL_VALUE_FILED);
        //如果不是原始日志类型
        if(!(originalObj instanceof OriginalSysLog))
            return;
        OriginalSysLog sysLog = (OriginalSysLog)originalObj;
        //构建特征码
        sysLog.generateFeatureCode();
        //构建SysLogType对象
        SysLogType logType = new SysLogType();
        logType.setAppId(sysLog.getAppId());
        logType.setLogMessage(sysLog.getLogMessage());
        logType.setCallStack(sysLog.getCallStack());
        logType.setFeatureCode(sysLog.getFeatureCode());
        logType.setLogLevel(sysLog.getLogLevel());
        logType.setRecentDate(sysLog.getLogDate());
        //获取logType的ID
        Integer typeId = logTypeManager.get(logType);
        //构建SysLogDetail对象,并推到下一个处理单元
        SysLogDetail detail = new SysLogDetail(typeId, sysLog.getAppId(), sysLog.getLogDate());
        //推出数据
        this.collector.emit(new Values(partitionKey, detail));
        //确认应答
        this.collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(
                new Fields(
                        ConstVariables.SYS_LOG_ORIGINAL_PARTITION_FIELD,
                        ConstVariables.SYS_LOG_DETAIL_VALUE_FILED));
    }
}
