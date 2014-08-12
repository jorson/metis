package com.metis.monitor.syslog.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.metis.monitor.syslog.config.SysLogConfig;
import com.metis.monitor.syslog.entry.SysLogDetail;
import com.metis.monitor.syslog.entry.SysLogMiniCycle;
import com.metis.monitor.syslog.util.C3P0Utils;
import com.metis.monitor.syslog.util.ConstVariables;
import com.rits.cloning.Cloner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Administrator on 14-8-6.
 */
public class BatchingBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(BatchingBolt.class);

    private OutputCollector collector;
    private static Integer intervalMs = 60000;
    private ConcurrentLinkedQueue<SysLogDetail> tupleQueue = new ConcurrentLinkedQueue<SysLogDetail>();
    private static Long currentTimestamp;
/*    private Connection connection;*/
    private SimpleDateFormat detailFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private SimpleDateFormat minCycleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:00");

    private ExecutorService executorService = null;
    private Cloner cloner;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.intervalMs = Integer.valueOf(map.get(SysLogConfig.MIN_INTERVAL_MS).toString());

        String driver = map.get(SysLogConfig.TARGET_DRIVER).toString();
        String url = map.get(SysLogConfig.TARGET_URL).toString();
        String user = map.get(SysLogConfig.TARGET_USER).toString();
        String password = map.get(SysLogConfig.TARGET_PASSWORD).toString();
        //初始化C3P0组件
        C3P0Utils.getInstance().init(driver, url, user, password);
        //初始化Executor
        executorService = Executors.newFixedThreadPool(10);
        cloner = new Cloner();
/*        //设置Connection
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, password);
            if(connection == null) {
                logger.info("BATCHING_BOLT", "Init Connection Error!!");
            }
        } catch (Exception e) {
            if(logger.isErrorEnabled()) {
                logger.error("BATCHING_BOLT", e);
            }
        }*/
        //最后设置
        Calendar calendar = Calendar.getInstance();
        //将毫秒和秒都设置为0
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        currentTimestamp = calendar.getTimeInMillis();
    }

    @Override
    public void execute(Tuple tuple) {
        //获取从上个节点传递过来的
        Object detailObj = tuple.getValueByField(ConstVariables.SYS_LOG_DETAIL_VALUE_FILED);
        if(!(detailObj instanceof SysLogDetail)) {
            return;
        }
        SysLogDetail logDetail = (SysLogDetail)detailObj;
        //加入处理队列
        tupleQueue.add(logDetail);
        long logTime = logDetail.getLogDate().getTime();
        long currentTime = System.currentTimeMillis();

        logger.info(String.format("BatchingBolt, Interval:%d, CurrentTimestamp:%d, CurrentTime:%d, LogTime:%d",
                intervalMs, currentTimestamp, currentTime, logTime));

        //如果日志时间达到上报间隔或自然流逝的时间达到时间间隔
        if(logTime >= intervalMs + currentTimestamp ||
                currentTime >= intervalMs + currentTimestamp) {
            //先将时间推进
            currentTimestamp += intervalMs;
            ConcurrentLinkedQueue<SysLogDetail> queue = cloner.deepClone(tupleQueue);
            //清空对象
            this.tupleQueue.clear();
            //启动线程
            executorService.execute(new ImportLogData(queue, logTime));
            //响应应答
/*            while(!tupleQueue.isEmpty()) {
                Tuple tup = tupleQueue.poll();
                collector.ack(tup);
            }*/

        }
        //直接应答
        this.collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        if(executorService != null) {
            try{
                while (!executorService.isTerminated()) {
                    executorService.awaitTermination(10, TimeUnit.SECONDS);
                }
            } catch (Exception ex) {
                if(logger.isErrorEnabled()) {
                    logger.error("BatchingBolt", ex);
                }
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    private String buildSysLogDetailSql(SysLogDetail entry) {
        String sql = String.format("Insert Into syslog_detail_data(AppId, TypeId, AddTime) Values(%d,%d,'%s');",
                entry.getAppId(),
                entry.getLogTypeId(),
                detailFormat.format(entry.getLogDate()));
        return sql;
    }

    private String buildSysLogMiniCycleSql(SysLogMiniCycle entry) {
        String sql = String.format("Insert Into syslog_min_cycle_data(StatTime, AppId, TypeId, LogAmount)" +
                " Values('%s',%d,%d,%d);",
                minCycleFormat.format(entry.getStatDate()),
                entry.getAppId(),
                entry.getTypeId(),
                entry.getLogAmount());
        return sql;
    }

    private class ImportLogData implements Runnable {

        private ConcurrentLinkedQueue<SysLogDetail> queue = null;
        private long logTime;

        public ImportLogData(ConcurrentLinkedQueue<SysLogDetail> queue, long logTime) {
            if(!queue.isEmpty()) {
                this.queue = queue;
            }
            this.logTime = logTime;
        }

        @Override
        public void run() {
            logger.info("BatchingBolt, Start Process:" + logTime);
            Connection connection = null;
            Statement statement = null;
            try{
                connection = C3P0Utils.getInstance().getConnection();
                if(connection == null) {
                    if(logger.isErrorEnabled()) {
                        logger.error("BATCHING_BOLT", "Get Connection from C3P0Utils IS NULL");
                    }
                    return;
                }
                connection.setAutoCommit(false);
                statement = connection.createStatement();
                SysLogDetail detail = null;
                Map<Integer, SysLogMiniCycle> miniCycleList = new HashMap<Integer, SysLogMiniCycle>();
                Integer checkFlag = 0;

                while (!queue.isEmpty()) {
                    detail = queue.poll();
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
                }

                for(SysLogMiniCycle val : miniCycleList.values()) {
                    statement.addBatch(buildSysLogMiniCycleSql(val));
                }
                //提交数据
                statement.executeBatch();
                connection.commit();
                connection.setAutoCommit(true);
                logger.info("BatchingBolt, EndProcess Process:" + currentTimestamp);
            } catch (Exception ex) {
                if(logger.isErrorEnabled()) {
                    logger.error("BATCHING_BOLT", ex);
                }
            } finally {
                if(statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        if(logger.isErrorEnabled()) {
                            logger.error("BATCHING_BOLT", "Close Statement from C3P0Utils IS NULL");
                        }
                    }
                }
                if(connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        if(logger.isErrorEnabled()) {
                            logger.error("BATCHING_BOLT", "Close Connection from C3P0Utils IS NULL");
                        }
                    }
                }
            }
        }
    }
}
