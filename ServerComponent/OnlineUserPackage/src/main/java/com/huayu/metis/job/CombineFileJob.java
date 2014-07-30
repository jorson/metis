package com.huayu.metis.job;

import com.huayu.metis.config.UserOnlinePackageConfig;
import com.huayu.metis.entry.RegisterLogEntry;
import com.huayu.metis.entry.VisitLogEntry;
import com.huayu.metis.io.SequenceFileCombiner;
import com.mongodb.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.status_jsp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * 将每天的文件进行合并的作业
 * Created by Administrator on 14-7-28.
 */
public class CombineFileJob extends BasicJob {

    private static Logger logger = LoggerFactory.getLogger(CombineFileJob.class);

    private DBObject lastExecuteObj;
    private String lastExecuteDate;

    private String originalPath, combinePath;


    private SimpleDateFormat format=new SimpleDateFormat("yyyyMMdd");

    private final String LOG_TYPE_KEY = "log_key";
    private final String COMBINE_DATE_KEY = "last_combine_date";
    //标记配置是否被加载过
    private static boolean configHasLoad = false;
    /**
     * 合并日志文件
     */
    public CombineFileJob(String configPath) {
        loadJobConfig(configPath);
    }

    //获取最后执行的日期
    private void loadLastExecuteDate(String key) {
        lastExecuteObj = collection.findOne(new BasicDBObject(LOG_TYPE_KEY, key));
        if(lastExecuteObj != null) {
            lastExecuteDate = lastExecuteObj.get(COMBINE_DATE_KEY).toString();
        }
    }

    //写入最后执行的时间
    private void writeLastExecuteDate(String key, String lastExecuteDate) {
        if(lastExecuteObj != null) {
            DBObject condition = new BasicDBObject();
            condition.put(LOG_TYPE_KEY, key);
            lastExecuteObj.put(COMBINE_DATE_KEY, lastExecuteDate);
            collection.update(condition,
                    lastExecuteObj,
                    false,
                    false);
        }
    }

    /**
     * 合并每天的文件
     */
    private void combineDailyFile(Configuration conf, Calendar executeDate, String logKey) throws Exception {
        //获取目标文件
        URI targetFileUri = new URI(String.format("%s/daily/%s/%s-log.seq",
                combinePath, format.format(executeDate.getTime()), logKey));
        //如果文件不存在,开始创建目录
        URI srcPathUri = new URI(String.format("%s/%s/%s", originalPath,
                format.format(executeDate.getTime()), logKey));
        //开始合并文件, 根据输入的Key不同
        if(logKey.equalsIgnoreCase("visit")) {
            SequenceFileCombiner.combineFile(conf, srcPathUri, targetFileUri, VisitLogEntry.class);
        } else if(logKey.equalsIgnoreCase("register")) {
            SequenceFileCombiner.combineFile(conf, srcPathUri, targetFileUri, RegisterLogEntry.class);
        }
    }

    /**
     * 合并一周的文件
     */
    private void combineWeekFile(Configuration conf, Calendar executeDate, String logKey) throws Exception {
        Calendar lastDayOfWeek = Calendar.getInstance();
        //设置周的开始为周一
        lastDayOfWeek.setFirstDayOfWeek(Calendar.MONDAY);
        lastDayOfWeek.set(executeDate.get(Calendar.YEAR),
                executeDate.get(Calendar.MONTH),
                executeDate.get(Calendar.DATE));
        //倒退一天
        lastDayOfWeek.add(Calendar.DAY_OF_MONTH, -1);
        int weekOfYear = lastDayOfWeek.getWeekYear();

        List<URI> lstUri = new ArrayList<URI>(7);
        while (lastDayOfWeek.getWeeksInWeekYear() == weekOfYear) {
            lstUri.add(new URI(String.format("%s/daily/%s/%s-log.seq",
                    combinePath, format.format(lastDayOfWeek.getTime()),
                    logKey)));
            lastDayOfWeek.add(Calendar.DAY_OF_MONTH, -1);
        }
        URI targetFile = new URI(String.format("%s/week/%d%d/%s-log.seq",
                combinePath, executeDate.get(Calendar.YEAR),
                weekOfYear, logKey));
        //开始合并文件, 根据输入的Key不同
        if(logKey.equalsIgnoreCase("visit")) {
            SequenceFileCombiner.combineFile(conf, lstUri, targetFile, VisitLogEntry.class);
        } else if(logKey.equalsIgnoreCase("register")) {
            SequenceFileCombiner.combineFile(conf, lstUri, targetFile, RegisterLogEntry.class);
        }
    }

    /**
     * 合并一个月的文件
     */
    public void combineMonthFile(Configuration conf, Calendar executeDate, String logKey) throws Exception{
        Calendar lastDayOfWeek = Calendar.getInstance();
        lastDayOfWeek.set(executeDate.get(Calendar.YEAR),
                executeDate.get(Calendar.MONTH),
                executeDate.get(Calendar.DATE));
        //倒退一天
        lastDayOfWeek.add(Calendar.DAY_OF_MONTH, -1);
        int monthOfYear = lastDayOfWeek.get(Calendar.MONTH);

        List<URI> lstUri = new ArrayList<URI>(7);
        while (lastDayOfWeek.get(Calendar.MONTH) == monthOfYear) {
            lstUri.add(new URI(String.format("%s/daily/%s/%s-log.seq",
                    combinePath, format.format(lastDayOfWeek.getTime()),
                    logKey)));
            lastDayOfWeek.add(Calendar.DAY_OF_MONTH, -1);
        }
        URI targetFile = new URI(String.format("%s/month/%d%d/%s-log.seq",
                combinePath, executeDate.get(Calendar.YEAR),
                monthOfYear, logKey));
        //开始合并文件, 根据输入的Key不同
        if(logKey.equalsIgnoreCase("visit")) {
            SequenceFileCombiner.combineFile(conf, lstUri, targetFile, VisitLogEntry.class);
        } else if(logKey.equalsIgnoreCase("register")) {
            SequenceFileCombiner.combineFile(conf, lstUri, targetFile, RegisterLogEntry.class);
        }
    }

    @Override
    public int runJob(String[] args) {
        if(args.length != 1) {
            logger.error("please enter log type to combine!");
            System.out.println("please enter log type to combine!");
            return 1;
        }

        int executeResult = 0;
        String logKey = args[0];
        try {
            //初始化Mongo对象
            initMongo();
            //读取需要执行的日期
            loadLastExecuteDate(logKey);
            //判断
            if(lastExecuteDate == null) {
                logger.error("can't read last execute date from control DB, Key=" + args[0]);
                System.out.println("can't read last execute date from control DB, Key=" + args[0]);
                return 0;
            }

            //开始循环
            Calendar start = Calendar.getInstance();
            Calendar end = Calendar.getInstance();
            start.setTime(format.parse(lastExecuteDate));
            //执行到昨天
            end.add(Calendar.DATE, -1);
            //声明配置
            Configuration conf = new Configuration();
            //循环执行
            while (start.before(end)) {
                //执行当天的合并任务
                combineDailyFile(conf, start, logKey);
                //向前推进一天
                start.add(Calendar.DAY_OF_MONTH, 1);
                //如果推进一天后是周一, 需要合并一周的文件
                if(start.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY){
                    combineWeekFile(conf, start, logKey);
                }
                //如果推进一天后是1号, 需要合并一月的文件
                if(start.get(Calendar.DAY_OF_MONTH) == 1) {
                    combineMonthFile(conf, start, logKey);
                }
            }
            //写入最后执行的日期
            writeLastExecuteDate(logKey, format.format(start.getTime()));
        } catch (Exception ex) {
            logger.error("COMBINE_JOB", ex);
            ex.printStackTrace();
        }
        return executeResult;
    }

    @Override
    protected void loadJobConfig(String configPath) {
        try {
            if(configHasLoad) {
                return;
            }

            //初始化读取配置
            UserOnlinePackageConfig.getInstance().loadConfig(configPath);
            //设置变量
            combinePath = UserOnlinePackageConfig.getInstance()
                    .tryGet(UserOnlinePackageConfig.COMBINE_PATH);
            originalPath = UserOnlinePackageConfig.getInstance()
                    .tryGet(UserOnlinePackageConfig.ORIGINAL_PATH);
            //标记配置以及被加载
            configHasLoad = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
