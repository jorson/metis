package com.huayu.metis.job;

import com.huayu.metis.config.UserOnlinePackageConfig;
import com.huayu.metis.io.CustomDbOutputFormat;
import com.huayu.metis.io.VisitLogInputFormat;
import com.huayu.metis.keyvalue.usage.UserPageVisitKey;
import com.huayu.metis.mr.usage.UserPageVisitMapReduce;
import com.mongodb.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * 用户页面访问
 * Created by Administrator on 14-7-28.
 */
public class UserPageVisitJob extends BasicJob {

    private static final Logger logger = LoggerFactory.getLogger(UserPageVisitJob.class);
    private static String lastExecuteDate;
    private static DBObject lastExecuteObj;
    private String combinePath, targetDriver, targetConnect, targetUser, targetPassword;

    private static final String LOG_TYPE_KEY = "log_key";
    private static final String EXECUTE_DATE_KEY = "last_execute_date";
    //标记配置是否被加载过
    private static boolean configHasLoad = false;

    /**
     * 用户访问页面次数的作业
     * @param configPath 作业的配置文件
     */
    public UserPageVisitJob(String configPath) {
        loadJobConfig(configPath);
    }

    //获取最后执行的日期
    public void loadLastExecuteDate(String key) {
        lastExecuteObj = collection.findOne(new BasicDBObject(LOG_TYPE_KEY, key));
        if(lastExecuteObj != null) {
            lastExecuteDate = lastExecuteObj.get(EXECUTE_DATE_KEY).toString();
        }
    }

    //写入最后执行的时间, 如果存在就更新, 不存在就添加
    public void writeLastExecuteDate(String key, String lastExecuteDate) {
        if(lastExecuteObj != null) {
            DBObject condition = new BasicDBObject();
            condition.put(LOG_TYPE_KEY, key);
            lastExecuteObj.put(EXECUTE_DATE_KEY, lastExecuteDate);
            collection.update(condition,
                    lastExecuteObj,
                    false,
                    false);
        }
    }

    @Override
    public int runJob(String[] args) {
        int executeResult = 0;
        try{
            //初始化Mongo对象
            initMongo();
            //读取需要执行的日期
            loadLastExecuteDate("visit");
            //开始循环
            Calendar start = Calendar.getInstance();
            Calendar end = Calendar.getInstance();
            start.setTime(format.parse(lastExecuteDate));
            //执行到昨天
            end.add(Calendar.DATE, -1);
            //声明配置
            Configuration conf = new Configuration();
            //文件系统
            FileSystem fs = FileSystem.get(conf);
            DBConfiguration.configureDB(conf, targetDriver, targetConnect, targetUser, targetPassword);

            while (start.before(end)) {
                Path inputPath = new Path(String.format("%s/daily/%s/visit-log.seq", combinePath,
                        format.format(start.getTime())));
                //如果文件不存在就直接忽略掉
                if(!fs.exists(inputPath)) {
                    logger.info("UserPageVisitJob", "combine visit log file not exists, date is " +
                            format.format(start.getTime()));
                    start.add(Calendar.DAY_OF_MONTH,1);
                    continue;
                }
                Job job = Job.getInstance(conf);
                job.setJobName("user.page.visit." + format.format(start.getTime()));
                job.setJarByClass(UserPageVisitJob.class);
                //设置Mapper
                job.setMapperClass(UserPageVisitMapReduce.UserVisitPageMapper.class);
                //设置Reducer
                job.setReducerClass(UserPageVisitMapReduce.UserVisitPageReducer.class);
                job.setNumReduceTasks(1);
                //设置Job输出的Key
                job.setOutputKeyClass(UserPageVisitKey.class);
                job.setOutputValueClass(IntWritable.class);
                //设置输入和输出格式类
                job.setInputFormatClass(SequenceFileInputFormat.class);
                //job.setOutputFormatClass(DBOutputFormat.class);
                job.setOutputFormatClass(CustomDbOutputFormat.class);
                //设置输入格式源
                SequenceFileInputFormat.addInputPath(job, inputPath);
                //设置输出格式源
                CustomDbOutputFormat.setOutput(job,
                        "user_visit_interval",
                        new String[] {"StatisticDate", "AppId", "TerminalCode", "UserId", "Visits"});
/*                DBOutputFormat.setOutput(job, "user_visit_interval",
                        new String[] {"StatisticDate", "AppId", "TerminalCode", "UserId", "Visits"});*/
                //开始执行...直到结束
                executeResult = job.waitForCompletion(true) ? 0 : 1;
                //继续下一个循环
                start.add(Calendar.DAY_OF_MONTH,1);
            }
            //写入最后执行的日期
            writeLastExecuteDate("visit", format.format(start.getTime()));
        } catch (Exception ex) {
            logger.error("UserPageVisitJob", ex);
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
}
