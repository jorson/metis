package com.huayu.metis.mr;

import com.huayu.metis.entry.RegisterLogEntry;
import com.huayu.metis.keyvalue.RegisterUserKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Random;

/**
 * 注册相关的MR方法测试
 * Created by Administrator on 14-7-18.
 */
public class RegisterMapReduceTest {

    private MapReduceDriver<LongWritable, RegisterLogEntry, RegisterUserKey,
            LongWritable, RegisterUserKey, IntWritable> mrRegister;

    @Before
    public void setup() {
        mrRegister = new MapReduceDriver<LongWritable, RegisterLogEntry, RegisterUserKey,
                LongWritable, RegisterUserKey, IntWritable>();
        mrRegister.setMapper(new RegisterMapReduce.RegisterMapper());
        mrRegister.setReducer(new RegisterMapReduce.RegisterReducer());
    }

    @Test
    public void calenderTest() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        System.out.println(cal.getTimeInMillis());

        GregorianCalendar gCal = new GregorianCalendar(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DATE));
        System.out.println(gCal.getTimeInMillis());
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

            String orgText = String.format("auc\t%d\t%d\t1\t1001\t3264456\t2014-07-17 15:15:15", i, appIds[order].intValue());
            RegisterLogEntry entry = new RegisterLogEntry();
            entry.parse(orgText);

            mrRegister.addInput(new LongWritable(counter), entry);
        }

        //开始计算
        List<Pair<RegisterUserKey, IntWritable>> results = mrRegister.run(true);
        for(int i=0; i<results.size(); i++){
            Pair<RegisterUserKey, IntWritable> pair = results.get(i);
            System.out.println("Key:" + pair.getFirst().toString() + ", Value:" + pair.getSecond().get());
        }
    }
}
