package com.huayu.metis.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by Administrator on 14-7-22.
 */
public class UseRageJob {

    public void runJob() throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        MultipleOutputs.addNamedOutput(job, "", TextOutputFormat.class, Text.class, Text.class);
    }
}
