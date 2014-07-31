package com.huayu.metis.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.util.ArrayList;

/**
 * 页面使用情况的JOB
 * Created by Administrator on 14-7-22.
 */
public class PageUseRateJob extends BasicJob {

    @Override
    public int runJob(String[] args) {
        try{
            Configuration conf = new Configuration();
            //设置两个JOB
            Job useJob = Job.getInstance(conf);
            Job rateJob = Job.getInstance(conf);
            //设置两个JOB的依赖
            //ControlledJob ctrlUseJob = new ControlledJob(rateJob, C)



            ControlledJob userUsePageJob = new ControlledJob(conf);
            ControlledJob usePageRateJob = new ControlledJob(conf);
            //其中后一个JOB依赖于前一个JOB的完成
            usePageRateJob.addDependingJob(userUsePageJob);

            userUsePageJob.setJobName("");
            usePageRateJob.setJobName("");



            JobControl control = new JobControl("user.user.page.rate");
            control.addJob(userUsePageJob);
            control.addJob(usePageRateJob);
            control.run();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return 0;

    }

    @Override
    protected void loadJobConfig(String configPath) {

    }



}
