package com.huayu.metis.job;

import com.huayu.metis.config.UserOnlinePackageConfig;
import com.huayu.metis.io.CustomDbOutputFormat;
import com.huayu.metis.keyvalue.usage.PageVisitKey;
import com.huayu.metis.keyvalue.usage.PageVisitOutputValue;
import com.huayu.metis.keyvalue.usage.UserPageVisitKey;
import com.huayu.metis.mr.usage.PageVisitMapReduce;
import com.huayu.metis.mr.usage.UserPageVisitMapReduce;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * 页面访问情况的MR作业类
 * Created by Administrator on 14-7-30.
 */
public class PageVisitJob extends BasicJob {

    private static final Logger logger = LoggerFactory.getLogger(PageVisitJob.class);
    private static String lastExecuteDate;
    private static DBObject lastExecuteObj;
    private String combinePath, targetDriver, targetConnect, targetUser, targetPassword;

    private static final String JOB_TYPE_KEY = "job_key";
    private static final String EXECUTE_DATE_KEY = "last_execute_date";
    //标记配置是否被加载过
    private static boolean configHasLoad = false;

    public PageVisitJob(String configPath) {
        loadJobConfig(configPath);
    }

    //如果正常运行,每天执行一个JOB,每周一执行2个JOB,每月第一天执行3个JOB
    @Override
    public int runJob(String[] args) {
        try{
            //初始化Mongo对象
            initMongo();
            //读取需要执行的日期
            loadLastExecuteDate("page_visit");
            //开始循环
            Calendar start = Calendar.getInstance();
            Calendar end = Calendar.getInstance();
            //设置日历
            start.setFirstDayOfWeek(Calendar.MONDAY);
            end.setFirstDayOfWeek(Calendar.MONDAY);

            start.setTime(format.parse(lastExecuteDate));
            //执行到昨天
            end.add(Calendar.DATE, -1);
            //声明配置
            Configuration conf = new Configuration();
            //先将配置设置为none
            conf.set("custom.period", "none");
            //文件系统
            FileSystem fs = FileSystem.get(conf);
            DBConfiguration.configureDB(conf, targetDriver, targetConnect, targetUser, targetPassword);

            while (start.before(end)){
                //执行当天的任务
                executeJob(conf, fs, "day", start);
                //向前推进一天
                start.add(Calendar.DAY_OF_MONTH, 1);
                //如果推进一天后是周一, 需要合并一周的文件
                if(start.get(Calendar.DAY_OF_WEEK) == Calendar.MONDAY){
                    executeJob(conf, fs, "week", start);
                }
                //如果推进一天后是1号, 需要合并一月的文件
                if(start.get(Calendar.DAY_OF_MONTH) == 1) {
                    executeJob(conf, fs, "month", start);
                }
            }
            //写入最后执行的日期
            writeLastExecuteDate("page_visit", format.format(start.getTime()));
        } catch (Exception ex) {
            logger.error("PageVisitJob", ex);
            ex.printStackTrace();
        }


        return 0;
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
            targetDriver = UserOnlinePackageConfig.getInstance()
                    .tryGet(UserOnlinePackageConfig.TARGET_DRIVER);
            targetConnect = UserOnlinePackageConfig.getInstance()
                    .tryGet(UserOnlinePackageConfig.TARGET_CONNECT);
            targetUser = UserOnlinePackageConfig.getInstance()
                    .tryGet(UserOnlinePackageConfig.TARGET_USER);
            targetPassword = UserOnlinePackageConfig.getInstance()
                    .tryGet(UserOnlinePackageConfig.TARGET_PASSWORD);
            //标记配置以及被加载
            configHasLoad = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private int executeJob(Configuration conf, FileSystem fs, String periodType, Calendar executeDate)
            throws Exception {
        Path inputPath = null;
        //倒退一天
        executeDate.add(Calendar.DAY_OF_MONTH, -1);

        if(periodType.equalsIgnoreCase("day")) {
            conf.set("custom.period", periodType);
            inputPath = new Path(String.format("%s/daily/%s/visit-log.seq", combinePath,
                    format.format(executeDate.getTime())));
        } else if (periodType.equalsIgnoreCase("week")) {
            conf.set("custom.period", periodType);
            int weekOfYear = executeDate.getWeekYear();
            inputPath = new Path(String.format("%s/week/%d%d/visit-log.seq", combinePath,
                    executeDate.get(Calendar.YEAR),
                    weekOfYear));
        } else if (periodType.equalsIgnoreCase("month")) {
            conf.set("custom.period", periodType);
            int monthOfYear = executeDate.get(Calendar.MONTH);
            inputPath = new Path(String.format("%s/month/%d%d/visit-log.seq", combinePath,
                    executeDate.get(Calendar.YEAR),
                    monthOfYear));
        } else{
            return 1;
        }

        //如果文件不存在就直接忽略掉
        if(!fs.exists(inputPath)) {
            logger.info("UserPageVisitJob", "combine visit log file not exists, date is " +
                    format.format(executeDate.getTime()));
            return 1;
        }

        Job job = Job.getInstance(conf);
        job.setJobName("page.visit." + format.format(executeDate.getTime()));
        job.setJarByClass(UserPageVisitJob.class);
        //设置Mapper
        job.setMapperClass(PageVisitMapReduce.PageVisitMapper.class);
        //设置Reducer
        job.setReducerClass(PageVisitMapReduce.PageVisitReducer.class);
        job.setNumReduceTasks(1);
        //设置Job输出的Key
        job.setOutputKeyClass(PageVisitKey.class);
        job.setOutputValueClass(PageVisitOutputValue.class);
        //设置输入和输出格式类
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(CustomDbOutputFormat.class);
        //设置输入格式源
        SequenceFileInputFormat.addInputPath(job, inputPath);
        //设置输出格式源
        CustomDbOutputFormat.setOutput(job,
                "visit_page_url_trends",
                new String[] {"StartDate", "EndDate", "PeriodType", "AppId",
                        "TerminalCode", "PageUrl", "VisitTimes", "UserAmount"});

        //开始执行...直到结束
        return job.waitForCompletion(true) ? 0 : 1;
    }

    //获取最后执行的日期
    private void loadLastExecuteDate(String key) {
        lastExecuteObj = collection.findOne(new BasicDBObject(JOB_TYPE_KEY, key));
        if(lastExecuteObj != null) {
            lastExecuteDate = lastExecuteObj.get(EXECUTE_DATE_KEY).toString();
        }
    }

    //写入最后执行的时间, 如果存在就更新, 不存在就添加
    private void writeLastExecuteDate(String key, String lastExecuteDate) {
        if(lastExecuteObj != null) {
            DBObject condition = new BasicDBObject();
            condition.put(JOB_TYPE_KEY, key);
            lastExecuteObj.put(EXECUTE_DATE_KEY, lastExecuteDate);
            collection.update(condition,
                    lastExecuteObj,
                    false,
                    false);
        }
    }
}
