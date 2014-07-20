package com.huayu.metis.mr;

import com.huayu.metis.entry.FileOffset;
import com.huayu.metis.entry.RegisterLogEntry;
import com.huayu.metis.keyvalue.RegisterUserKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * 计算当天注册用户量的MR
 * Created by Administrator on 14-7-14.
 */
public class RegisterMapReduce {

    private static final Logger logger = LoggerFactory.getLogger(RegisterMapReduce.class);

    public static class RegisterMapper extends Mapper<LongWritable, RegisterLogEntry, RegisterUserKey, LongWritable> {
        private RegisterUserKey writableKey = new RegisterUserKey();
        private LongWritable writableValue = new LongWritable(1);

        @Override
        protected void map(LongWritable key, RegisterLogEntry value, Context context) throws IOException, InterruptedException {
            //去掉注册日期中的时间部分
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(value.getRegisterTime());
            GregorianCalendar gCal = new GregorianCalendar(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DATE));

            //设置键中的各个字段信息
            writableKey.setStartDate(new LongWritable(gCal.getTimeInMillis()));
            writableKey.setEndDate(new LongWritable(gCal.getTimeInMillis()));
            writableKey.setPeriodType(new IntWritable(1));
            writableKey.setAppId(new IntWritable(value.getAppId()));
            writableKey.setTerminalCode(new IntWritable(value.getTerminalCode()));

            context.write(writableKey, writableValue);
        }
    }

    public static class RegisterReducer extends Reducer<RegisterUserKey, LongWritable, RegisterUserKey, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(RegisterUserKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for(LongWritable val : values) {
                sum += val.get();
            }
            result.set((int)sum);
            context.write(key, result);
        }
    }
}
