package com.metis.monitor.syslog.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.metis.monitor.syslog.config.SysLogConfig;
import com.metis.monitor.syslog.entry.OriginalSysLog;
import com.metis.monitor.syslog.entry.SysLogDetail;
import com.metis.monitor.syslog.entry.SysLogType;
import com.metis.monitor.syslog.util.C3P0Utils;
import com.metis.monitor.syslog.util.ConstVariables;
import com.metis.monitor.syslog.util.SysLogTypeManager;
import com.metis.monitor.syslog.util.SysLogTypeMissing;
import com.metis.monitor.syslog.util.redis.RedisClient;
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
        String driver = map.get(SysLogConfig.TARGET_DRIVER).toString();
        String url = map.get(SysLogConfig.TARGET_URL).toString();
        String user = map.get(SysLogConfig.TARGET_USER).toString();
        String password = map.get(SysLogConfig.TARGET_PASSWORD).toString();
        String cacheHost = map.get(SysLogConfig.CACHE_HOST).toString();
        String cachePort = map.get(SysLogConfig.CACHE_PORT).toString();
        String cacheCatalog = map.get(SysLogConfig.CACHE_CATALOG).toString();
        logger.info("TransportBolt cacheHost:" + cacheHost + ", cachePort:"
            + cachePort + ", cacheCatalog:" + cacheCatalog);
        //初始化C3P0组件
        C3P0Utils.getInstance().init(driver, url, user, password);
        //初始化RedisClient组件
        RedisClient.getInstance(cacheHost, Integer.parseInt(cachePort), Integer.parseInt(cacheCatalog));
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
        if(!(originalObj instanceof OriginalSysLog)){
            logger.warn("Not Instance Of ORIGINALSYSLOG");
            return;
        }

        try{
            OriginalSysLog sysLog = (OriginalSysLog)originalObj;
            //构建特征码
            sysLog.generateFeatureCode();
            logger.info("Transport Start, featureCode=" + sysLog.getFeatureCode());
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
            logger.info("Transport End, featureCode=" + sysLog.getFeatureCode());
        } catch (Exception ex) {
            logger.warn("Transport Error", ex);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(
                new Fields(
                        ConstVariables.SYS_LOG_ORIGINAL_PARTITION_FIELD,
                        ConstVariables.SYS_LOG_DETAIL_VALUE_FILED));
    }
}
