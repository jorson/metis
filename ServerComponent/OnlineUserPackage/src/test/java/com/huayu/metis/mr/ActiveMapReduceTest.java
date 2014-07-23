package com.huayu.metis.mr;

import com.huayu.metis.entry.VisitLogEntry;
import com.huayu.metis.keyvalue.trend.ActiveUserKey;
import com.huayu.metis.mr.trend.ActiveMapReduce;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Random;

/**
 * Created by Administrator on 14-7-18.
 */
public class ActiveMapReduceTest {
    private MapReduceDriver<LongWritable, VisitLogEntry, ActiveUserKey, IntWritable,
            ActiveUserKey, IntWritable> mrRegister;

    @Before
    public void setup() {
        mrRegister = new MapReduceDriver<LongWritable, VisitLogEntry, ActiveUserKey, IntWritable,
                ActiveUserKey, IntWritable>();
        mrRegister.setMapper(new ActiveMapReduce.ActiveMapper());
        mrRegister.setReducer(new ActiveMapReduce.ActiveReducer());
    }

    @Test
    public void testRegisterMR() throws ParseException, IOException {
        long counter = 0;
        Integer[] appIds = new Integer[]{1,2,3,4,5,6};
        Random rnd = new Random();

        //添加模拟的数据
        for(int i=0; i<100; i++) {
            counter++;
            int order = rnd.nextInt(5);

            String orgText = String.format("auc\t%d\t%d\t1001\t3264456\ttest\ttest\ttest\t2014-07-17 15:15:15",
                    i, appIds[order].intValue());
            VisitLogEntry entry = new VisitLogEntry();
            entry.parse(orgText);

            mrRegister.addInput(new LongWritable(counter), entry);
        }

        //开始计算
        List<Pair<ActiveUserKey, IntWritable>> results = mrRegister.run(true);
        for(int i=0; i<results.size(); i++){
            Pair<ActiveUserKey, IntWritable> pair = results.get(i);
            System.out.println("Key:" + pair.getFirst().toString() + ", Value:" + pair.getSecond().get());
        }
    }
}
