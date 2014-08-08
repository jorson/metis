package com.metis.monitor.syslog.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.metis.monitor.syslog.config.SysLogConfig;
import com.metis.monitor.syslog.entry.SysLogDetail;
import com.metis.monitor.syslog.entry.SysLogMiniCycle;
import com.metis.monitor.syslog.util.ConstVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by Administrator on 14-8-6.
 */
public class BatchingBolt implements IRichBolt {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(BatchingBolt.class);

    private OutputCollector collector;
    private Integer intervalMs = 60000;
    private Queue<Tuple> tupleQueue = new ConcurrentLinkedQueue<Tuple>();
    private Queue<SysLogDetail> logDetailQueue = new ConcurrentLinkedQueue<SysLogDetail>();
    private Long currentTimestamp;
    private Connection connection;
    private SimpleDateFormat detailFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    private SimpleDateFormat minCycleFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:00");

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.intervalMs = SysLogConfig.getInstance().tryGetInt(SysLogConfig.MIN_INTERVAL_MS, 60000);
        //设置Connection
        String driver = SysLogConfig.getInstance().tryGet(SysLogConfig.TARGET_DRIVER);
        String url = SysLogConfig.getInstance().tryGet(SysLogConfig.TARGET_URL);
        String user = SysLogConfig.getInstance().tryGet(SysLogConfig.TARGET_USER);
        String password = SysLogConfig.getInstance().tryGet(SysLogConfig.TARGET_PASSWORD);

        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //最后设置
        Calendar calendar = Calendar.getInstance();
        //将毫秒和秒都设置为0
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        this.currentTimestamp = calendar.getTimeInMillis();
    }

    @Override
    public void execute(Tuple tuple) {
        //获取从上个节点传递过来的
        Object detailObj = tuple.getValueByField(ConstVariables.SYS_LOG_DETAIL_VALUE_FILED);
        if(!(detailObj instanceof SysLogDetail)) {
            return;
        }
        //加入处理队列
        logDetailQueue.add((SysLogDetail)detailObj);
        tupleQueue.add(tuple);
        SysLogDetail logDetail = (SysLogDetail)detailObj;
        long logTime = logDetail.getLogDate().getTime();
        long currentTime = System.currentTimeMillis();
        //如果日志时间达到上报间隔或自然流逝的时间达到时间间隔
        if(logTime >= this.intervalMs + this.currentTimestamp ||
                currentTime >= this.intervalMs + this.currentTimestamp) {
            System.out.println("Start Process:" + logTime);
            try{
                Statement statement = connection.createStatement();
                connection.setAutoCommit(false);
                SysLogDetail detail = null;
                Tuple tup = null;
                Map<Integer, SysLogMiniCycle> miniCycleList = new HashMap<Integer, SysLogMiniCycle>();
                Integer checkFlag = 0;

                while (!logDetailQueue.isEmpty()) {
                    detail = logDetailQueue.poll();
                    tup = tupleQueue.poll();

                    statement.addBatch(buildSysLogDetailSql(detail));
                    //做GroupBY的操作
                    checkFlag = detail.getAppId() + detail.getLogTypeId();
                    if(miniCycleList.containsKey(checkFlag)) {
                        miniCycleList.get(checkFlag).addLogAmount();
                    } else {
                        SysLogMiniCycle cycle = new SysLogMiniCycle(detail.getLogDate(),
                                detail.getAppId(), detail.getLogTypeId());
                        miniCycleList.put(checkFlag, cycle);
                    }
                    collector.ack(tup);
                }

                for(SysLogMiniCycle val : miniCycleList.values()) {
                    statement.addBatch(buildSysLogMiniCycleSql(val));
                }
                //提交数据
                statement.executeBatch();
                connection.commit();
                connection.setAutoCommit(true);
                //推进时间
                this.currentTimestamp += this.intervalMs;
            } catch (Exception ex) {
                if(logger.isErrorEnabled()) {
                    logger.error("BATCHING_BOLT", ex);
                }
            }
        }
    }

    @Override
    public void cleanup() {
        this.logDetailQueue.clear();
        if(this.connection != null) {
            try {
                this.connection.close();
            } catch (SQLException ex) {
                if(logger.isErrorEnabled()) {
                    logger.error("BATCHING_BOLT", ex);
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private String buildSysLogDetailSql(SysLogDetail entry) {
        String sql = String.format("Insert Into syslog_detail_data(TypeId, AppId, AddTime) Values(%d,%d,'%s');",
                entry.getAppId(),
                entry.getLogTypeId(),
                detailFormat.format(entry.getLogDate()));
        return sql;
    }

    public String buildSysLogMiniCycleSql(SysLogMiniCycle entry) {
        String sql = String.format("Insert Into syslog_min_cycle_data(StatTime, AppId, TypeId, LogAmount)" +
                " Values('%s',%d,%d,%d);",
                minCycleFormat.format(entry.getStatDate()),
                entry.getAppId(),
                entry.getTypeId(),
                entry.getLogAmount());
        return sql;
    }
}
