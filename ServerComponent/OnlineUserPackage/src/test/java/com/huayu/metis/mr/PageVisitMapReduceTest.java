package com.huayu.metis.mr;

import com.huayu.metis.entry.VisitLogEntry;
import com.huayu.metis.keyvalue.usage.*;
import com.huayu.metis.mr.usage.PageVisitMapReduce;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Random;

/**
 * Created by Administrator on 14-7-30.
 */
public class PageVisitMapReduceTest {

    private MapReduceDriver<LongWritable, VisitLogEntry,
            PageVisitKey, PageVisitValue, PageVisitKey, PageVisitOutputValue> mrPageVisitor;

    @Before
    public void setup() {
        mrPageVisitor = new MapReduceDriver<LongWritable, VisitLogEntry, PageVisitKey,
                PageVisitValue, PageVisitKey, PageVisitOutputValue>();

        mrPageVisitor.setMapper(new PageVisitMapReduce.PageVisitMapper());
        mrPageVisitor.setReducer(new PageVisitMapReduce.PageVisitReducer());
    }

    @Test
    public void testVisitPageMR() throws Exception {
        //添加模拟数据
        generateVisitData();
        mrPageVisitor.getConfiguration().set("custom.period", "month");
        //开始计算
        List<Pair<PageVisitKey, PageVisitOutputValue>> results = mrPageVisitor.run(true);
        for(int i=0; i<results.size(); i++){
            Pair<PageVisitKey, PageVisitOutputValue> pair = results.get(i);
            System.out.println("Key:" + pair.getFirst().toString() + ", Value:" + pair.getSecond().toString());
        }
    }

    private void generateVisitData() throws Exception {
        long counter = 0;
        Integer[] appIds = new Integer[]{1,2,3,4,5,6};
        Long[] userIds = new Long[] {100011L,100010L,10090L,10008L,10007L,10006L,10005L,10004L,10003L,10002L};
        String[] visitTime = new String[]{"2014-07-05 15:15:15","2014-07-10 15:15:15","2014-07-15 15:15:15",
                "2014-07-20 15:15:15","2014-07-25 15:15:15","2014-07-30 15:15:15","2014-08-04 15:15:15",
                "2014-08-09 15:15:15","2014-08-14 15:15:15"};

        Random rnd = new Random();

        //添加模拟的数据
        for(int i=0; i<100; i++) {
            counter++;
            int order = rnd.nextInt(5);
            int uOrder = rnd.nextInt(9);

            String orgText = String.format("auc\t%d\t%d\t1001\t3264456\trefer.page\tvisit.page\tvisit.param\t%s",
                    userIds[uOrder], appIds[order].intValue(), visitTime[uOrder]);
            VisitLogEntry entry = new VisitLogEntry();
            entry.parse(orgText);

            mrPageVisitor.addInput(new LongWritable(counter), entry);
        }
    }
}
