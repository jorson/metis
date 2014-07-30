package com.huayu.metis;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

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

    @Test
    public void loopDate() {
        String str="20140714";
        //String str1="20110225";
        SimpleDateFormat format=new SimpleDateFormat("yyyyMMdd");
        Calendar start = Calendar.getInstance();
        Calendar end = Calendar.getInstance();
        try {
            start.setTime(format.parse(str));
            end.add(Calendar.DATE, -1);
            //end.setTime(format.parse(str1));
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        while(start.before(end))
        {
            System.out.println(format.format(start.getTime()));
            start.add(Calendar.DAY_OF_MONTH,1);
        }
    }
}
