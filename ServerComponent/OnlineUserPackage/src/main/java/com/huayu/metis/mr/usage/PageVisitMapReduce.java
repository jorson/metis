package com.huayu.metis.mr.usage;

import com.huayu.metis.entry.VisitLogEntry;
import com.huayu.metis.keyvalue.usage.UserPageVisitKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * 计算页面被访问次数与不重复访问用户数的MR
 * Created by Administrator on 14-7-22.
 */
public class PageVisitMapReduce {

    public static class UserVisitPageMapper extends Mapper<LongWritable, VisitLogEntry, UserPageVisitKey, IntWritable> {

        private UserPageVisitKey writableKey = new UserPageVisitKey();
        private IntWritable writableValue = new IntWritable(1);

        /**
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         * @should write to context
         */
        @Override
        protected void map(LongWritable key, VisitLogEntry value, Context context)
                throws IOException, InterruptedException {
            //如果用户的ID小于等于0, 直接忽略掉
            if(value.getUserId() <= 0) {
                return;
            }

            //去掉注册日期中的时间部分
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(value.getVisitTime());
            GregorianCalendar gCal = new GregorianCalendar(cal.get(Calendar.YEAR),
                    cal.get(Calendar.MONTH) + 1,
                    cal.get(Calendar.DATE));

            //设置键中的各个字段信息
            writableKey.setStatDate(gCal.getTimeInMillis());
            writableKey.setAppId(value.getAppId());
            writableKey.setTerminalCode(value.getTerminalCode());
            writableKey.setUserId(value.getUserId());

            context.write(writableKey, writableValue);
        }
    }

    public static class UserVisitPageReducer extends Reducer<UserPageVisitKey, IntWritable, UserPageVisitKey, IntWritable> {

        @Override
        protected void reduce(UserPageVisitKey key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for(IntWritable val : values){
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
