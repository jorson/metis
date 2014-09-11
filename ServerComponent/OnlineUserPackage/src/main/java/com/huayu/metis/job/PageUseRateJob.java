package com.huayu.metis.job;

import com.huayu.metis.config.UserOnlinePackageConfig;
import com.huayu.metis.io.CustomDbOutputFormat;
import com.huayu.metis.keyvalue.usage.UserUseKey;
import com.huayu.metis.keyvalue.usage.UserUseRateAmount;
import com.huayu.metis.keyvalue.usage.UserUseRateKey;
import com.huayu.metis.mr.usage.PageUseRateMapReduce;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;

/**
 * 页面使用情况的JOB
 * Created by Administrator on 14-7-22.
 */
public class PageUseRateJob extends BasicJob {

    private static final Logger logger = LoggerFactory.getLogger(PageUseRateJob.class);
    private static String lastExecuteDate;
    private static DBObject lastExecuteObj;
    private String combinePath, tempPath, targetDriver, targetConnect, targetUser, targetPassword;

    private static final String JOB_TYPE_KEY = "job_key";
    private static final String EXECUTE_DATE_KEY = "last_execute_date";
    //标记配置是否被加载过
    private static boolean configHasLoad = false;

    public PageUseRateJob(String configPath) {
        loadJobConfig(configPath);
    }

    @Override
    public int runJob(String[] args) {
        try {
            //初始化Mongo对象
            initMongo();
            //读取需要执行的日期
            loadLastExecuteDate("page_visit_rate");
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
            writeLastExecuteDate("page_visit_rate", format.format(start.getTime()));

        } catch (Exception ex) {
            logger.error("PageVisitRateJob", ex);
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
            tempPath = UserOnlinePackageConfig.getInstance()
                    .tryGet(UserOnlinePackageConfig.TEMP_PATH);
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

    //执行JOB
    private int executeJob(Configuration conf, FileSystem fs, String periodType, Calendar start)
            throws Exception {
        Path inputPath = null, tmpOutputPath = null;
        Calendar executeDate = Calendar.getInstance();
        executeDate.setTimeInMillis(start.getTimeInMillis());
        executeDate.setFirstDayOfWeek(Calendar.MONDAY);
        if(periodType.equalsIgnoreCase("day")) {
            conf.set("custom.period", periodType);
            inputPath = new Path(String.format("%s/daily/%s/visit-log.seq", combinePath,
                    format.format(executeDate.getTime())));
            tmpOutputPath = new Path(String.format("%s/daily/%s/visit-tmp-output", combinePath,
                    format.format(executeDate.getTime())));
        } else if (periodType.equalsIgnoreCase("week")) {
            //倒退一天
            executeDate.add(Calendar.DAY_OF_MONTH, -1);
            conf.set("custom.period", periodType);
            int weekOfYear = executeDate.get(Calendar.WEEK_OF_YEAR);
            inputPath = new Path(String.format("%s/week/%d%d/visit-log.seq", combinePath,
                    executeDate.get(Calendar.YEAR),
                    weekOfYear));
            tmpOutputPath = new Path(String.format("%s/week/%d%d/visit-tmp-output", combinePath,
                    executeDate.get(Calendar.YEAR),
                    weekOfYear));
        } else if (periodType.equalsIgnoreCase("month")) {
            //倒退一天
            executeDate.add(Calendar.DAY_OF_MONTH, -1);
            conf.set("custom.period", periodType);
            int monthOfYear = executeDate.get(Calendar.MONTH);
            inputPath = new Path(String.format("%s/month/%d%d/visit-log.seq", combinePath,
                    executeDate.get(Calendar.YEAR),
                    monthOfYear));
            tmpOutputPath = new Path(String.format("%s/month/%d%d/visit-tmp-output", combinePath,
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

        //开始执行作业
        //设置两个JOB
        Job useJob = Job.getInstance(conf);
        Job rateJob = Job.getInstance(conf);
        //设置两个JOB的依赖
        ControlledJob ctrlUseJob = new ControlledJob(conf);
        ctrlUseJob.setJob(useJob);
        ControlledJob ctrlRateJob = new ControlledJob(conf);
        ctrlRateJob.setJob(rateJob);
        //页面访问评论的JOB依赖于使用的JOB
        ctrlRateJob.addDependingJob(ctrlUseJob);

        //设置Job
        useJob.setJobName("page.visit.use." + format.format(executeDate.getTime()));
        useJob.setJarByClass(PageUseRateJob.class);
        //设置Mapper和Reducer
        useJob.setMapperClass(PageUseRateMapReduce.UserUsePageMapper.class);
        useJob.setReducerClass(PageUseRateMapReduce.UserUsePageReducer.class);
        //设置输出的格式
        useJob.setInputFormatClass(SequenceFileInputFormat.class);
        useJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        //设置输出键的类型
        useJob.setOutputKeyClass(UserUseKey.class);
        useJob.setOutputValueClass(IntWritable.class);
        //设置计算访问情况的作业的输入和输出
        SequenceFileInputFormat.addInputPath(useJob, inputPath);
        SequenceFileOutputFormat.setOutputPath(useJob, tmpOutputPath);


        rateJob.setJobName("page.visit.rate." + format.format(executeDate.getTime()));
        rateJob.setJarByClass(PageUseRateJob.class);
        //设置Mapper和Reducer
        rateJob.setMapperClass(PageUseRateMapReduce.UserUsePageRateMapper.class);
        rateJob.setReducerClass(PageUseRateMapReduce.UserUsePageRateReducer.class);
        //设置输出的格式
        rateJob.setInputFormatClass(SequenceFileInputFormat.class);
        rateJob.setOutputFormatClass(CustomDbOutputFormat.class);
        //设置MAP输出类型
        rateJob.setMapOutputKeyClass(UserUseRateKey.class);
        rateJob.setMapOutputValueClass(LongWritable.class);
        //设置输出键的类型
        rateJob.setOutputKeyClass(UserUseRateKey.class);
        rateJob.setOutputValueClass(UserUseRateAmount.class);
        //设置计算访问频率的作业的输入和输出
        SequenceFileInputFormat.addInputPath(rateJob, tmpOutputPath);
        CustomDbOutputFormat.setOutput(rateJob, "visit_page_num_trends",
                "StartDate", "EndDate", "PeriodType", "AppId", "TerminalCode", "PageNum", "TrendsValue", "TrendsType");
        //开始执行
        JobControl control = new JobControl("user.user.page.rate");
        control.addJob(ctrlUseJob);
        control.addJob(ctrlRateJob);

        Thread thread = new Thread(control);
        thread.start();
        while (true) {
            if(control.allFinished()){
                control.stop();
                return 0;
            }
            if(control.getFailedJobList().size() > 0){
                control.stop();
                return 1;
            }
        }
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
