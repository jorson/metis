package com.huayu.metis.mr;

import com.huayu.metis.entry.RegisterLogEntry;
import com.huayu.metis.keyvalue.trend.RegisterUserKey;
import com.huayu.metis.keyvalue.usage.UserUseKey;
import com.huayu.metis.keyvalue.usage.UserUseRateAmount;
import com.huayu.metis.keyvalue.usage.UserUseRateKey;
import com.huayu.metis.mr.usage.PageUseRateMapReduce;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

/**
 * @author wangchaoxu.
 */
public class UserUsePageRateMapReduceTest {

    private MapReduceDriver<UserUseKey, IntWritable, UserUseRateKey, LongWritable, UserUseRateKey, UserUseRateAmount> mrUserUsePage;

    @Before
    public void setup() {
        mrUserUsePage = new MapReduceDriver<UserUseKey, IntWritable, UserUseRateKey, LongWritable, UserUseRateKey, UserUseRateAmount>();
        mrUserUsePage.setMapper(new PageUseRateMapReduce.UserUsePageRateMapper());
        mrUserUsePage.setReducer(new PageUseRateMapReduce.UserUsePageRateReducer());
    }

    @Test
    public void userUserPageRateMRTest() throws IOException {
        long counter = 0;
        Integer[] appIds = new Integer[]{1,2,3,4,5,6};
        Random rnd = new Random();

        //添加模拟的数据
        for(int i=0; i<100; i++) {
            counter++;
            int order = rnd.nextInt(5);
            Calendar calendar = Calendar.getInstance();
            calendar.set(2014, 8, 18, 0, 0, 0);
            calendar.set(Calendar.MILLISECOND, 0);

            UserUseKey userUseKey = new UserUseKey();
            userUseKey.setAppId(appIds[order]);
            userUseKey.setUserId(rnd.nextInt(10000));
            userUseKey.setTerminalCode(1001);
            userUseKey.setPeriodType(0);
            userUseKey.setStartDate(calendar.getTimeInMillis());
            userUseKey.setEndDate(calendar.getTimeInMillis());

            mrUserUsePage.addInput(userUseKey, new IntWritable(rnd.nextInt(100)));
        }

        //开始计算
        List<Pair<UserUseRateKey, UserUseRateAmount>> results = mrUserUsePage.run(true);
        for(int i=0; i<results.size(); i++){
            Pair<UserUseRateKey, UserUseRateAmount> pair = results.get(i);
            System.out.println("Key:" + pair.getFirst().toString() + ", Value:" + pair.getSecond().get());
        }
    }
}
