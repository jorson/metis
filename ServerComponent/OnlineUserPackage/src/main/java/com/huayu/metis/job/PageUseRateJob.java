package com.huayu.metis.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * 页面使用情况的JOB
 * Created by Administrator on 14-7-22.
 */
public class PageUseRateJob extends BasicJob {

    @Override
    public int runJob(String[] args) {
        return 0;
    }

    @Override
    protected void loadJobConfig(String configPath) {

    }


}
