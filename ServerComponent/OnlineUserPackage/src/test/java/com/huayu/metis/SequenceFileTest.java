package com.huayu.metis;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by Administrator on 14-7-12.
 */
public class SequenceFileTest {

    public void write2SequenceFile() {

    }

    private static Configuration getDefaultConf() {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        return conf;
    }
}
