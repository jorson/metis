package com.metis.monitor.syslog.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;
import com.metis.monitor.syslog.entry.OriginalSysLog;
import com.metis.monitor.syslog.util.ConstVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * 将写入Kafka的系统日志原始字符串转换为OriginalSysLog对象
 * Created by Administrator on 14-8-8.
 */
public class OriginalParseBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(OriginalParseBolt.class);
    private static final long serialVersionUID = 1L;
    private final SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    private OutputCollector collector;

    public OriginalParseBolt() {

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext,
                        OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        //获取从KafkaSpout中传递过来的数据
        //String originalString = tuple.getValue(0).toString();
        String originalString = tuple.getValues().get(0).toString();
        try{
            originalString = URLDecoder.decode(originalString, "UTF-8");
        } catch(Exception ex) {
            if(logger.isErrorEnabled()) {
                logger.error("OriginalParseBolt", ex);
            }
            this.collector.fail(tuple);
            //出现异常, 记录日志后 忽略
            return;
        }

        //用\t分割
        String[] originalArray = originalString.split("\t");
        logger.info("ReMsg:" + originalString + ", Split:" + originalArray.length);
        //如果长度不是5
        if(originalArray.length != 5) {
            return;
        }
        try {
            Integer appId = Integer.parseInt(originalArray[0]);
            OriginalSysLog sysLog = new OriginalSysLog();
            sysLog.setAppId(appId);
            sysLog.setLogLevel(Integer.parseInt(originalArray[1]));
            sysLog.setLogMessage(originalArray[2]);
            sysLog.setCallStack(originalArray[3]);
            sysLog.setLogDate(format.parse(originalArray[4]));
            //将转换后的对象发出,同时发出AppId作为后续分组的依据
            this.collector.emit(new Values(appId, sysLog));
            this.collector.ack(tuple);
            logger.info("OriginalParseBolt", "End Process," + originalArray[4]);
        } catch(Exception ex) {
            if(logger.isErrorEnabled()) {
                logger.error("OriginalParseBolt", ex);
            }
            this.collector.fail(tuple);
            //出现异常, 记录日志后 忽略
            return;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(
                new Fields(
                        ConstVariables.SYS_LOG_ORIGINAL_PARTITION_FIELD,
                        ConstVariables.SYS_LOG_ORIGINAL_VALUE_FILED));
    }
}
