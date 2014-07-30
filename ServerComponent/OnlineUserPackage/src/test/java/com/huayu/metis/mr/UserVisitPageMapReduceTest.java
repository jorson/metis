package com.huayu.metis.mr;

import com.huayu.metis.entry.VisitLogEntry;
import com.huayu.metis.keyvalue.trend.RegisterUserKey;
import com.huayu.metis.keyvalue.usage.UserPageVisitKey;
import com.huayu.metis.keyvalue.usage.UserPageVisitTimes;
import com.huayu.metis.mr.usage.PageVisitMapReduce;
import com.huayu.metis.mr.usage.UserPageVisitMapReduce;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Random;

/**
 * Created by Administrator on 14-7-28.
 */
public class UserVisitPageMapReduceTest {

    private MapReduceDriver<LongWritable, VisitLogEntry,
            UserPageVisitKey, IntWritable, UserPageVisitKey, UserPageVisitTimes> mrUserVisitor;

    @Before
    public void setup() {
        mrUserVisitor = new MapReduceDriver<LongWritable, VisitLogEntry, UserPageVisitKey,
                IntWritable, UserPageVisitKey, UserPageVisitTimes>();
        mrUserVisitor.setMapper(new UserPageVisitMapReduce.UserVisitPageMapper());
        mrUserVisitor.setReducer(new UserPageVisitMapReduce.UserVisitPageReducer());
    }

    @Test
    public void testUserVisitPageMR() throws Exception {
        //添加模拟数据
        generateVisitData();

        //开始计算
        List<Pair<UserPageVisitKey, UserPageVisitTimes>> results = mrUserVisitor.run(true);
        for(int i=0; i<results.size(); i++){
            Pair<UserPageVisitKey, UserPageVisitTimes> pair = results.get(i);
            System.out.println("Key:" + pair.getFirst().toString() + ", Value:" + pair.getSecond().get());
        }
    }

    private void generateVisitData() throws Exception {
        long counter = 0;
        Integer[] appIds = new Integer[]{1,2,3,4,5,6};
        Long[] userIds = new Long[] {100011L,100010L,10090L,10008L,10007L,10006L,10005L,10004L,10003L,10002L};
        Random rnd = new Random();

        //添加模拟的数据
        for(int i=0; i<100; i++) {
            counter++;
            int order = rnd.nextInt(5);
            int uOrder = rnd.nextInt(9);

            String orgText = String.format("auc\t%d\t%d\t1001\t3264456\trefer.page\tvisit.page\tvisit.param\t2014-07-17 15:15:15",
                    userIds[uOrder], appIds[order].intValue());
            VisitLogEntry entry = new VisitLogEntry();
            entry.parse(orgText);

            mrUserVisitor.addInput(new LongWritable(counter), entry);
        }
    }
}
