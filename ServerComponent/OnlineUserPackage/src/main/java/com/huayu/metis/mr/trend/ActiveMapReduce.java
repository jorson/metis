package com.huayu.metis.mr.trend;

import com.huayu.metis.entry.VisitLogEntry;
import com.huayu.metis.keyvalue.trend.ActiveUserKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * 计算当天活跃用户的MR
 * Created by Administrator on 14-7-18.
 */
public class ActiveMapReduce {

    private static final Logger logger = LoggerFactory.getLogger(ActiveMapReduce.class);

    public static class ActiveMapper extends Mapper<LongWritable, VisitLogEntry, ActiveUserKey, IntWritable> {
        private ActiveUserKey writableKey = new ActiveUserKey();
        private IntWritable writableValue = new IntWritable(0);

        @Override
        protected void map(LongWritable key, VisitLogEntry value, Context context) throws IOException, InterruptedException {
            /*TO DOC
            * 从访问日志中提取UserId, AppId, TerminalCode, VisitTime; 将AppId和TerminalCode设置到ActiveUserKey中
            * 将UserId设置为输出的Value
            * */
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(value.getVisitTime());
            GregorianCalendar gCal = new GregorianCalendar(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DATE));

            //设置输出键的信息
            writableKey.setStartDate(new LongWritable(gCal.getTimeInMillis()));
            writableKey.setEndDate(new LongWritable(gCal.getTimeInMillis()));
            writableKey.setPeriodType(new IntWritable(1));
            writableKey.setAppId(new IntWritable(value.getAppId()));
            writableKey.setTerminalCode(new IntWritable(value.getTerminalCode()));
            //将用户ID设置为输出的Value
            writableValue.set(value.getUserId());

            context.write(writableKey, writableValue);
        }
    }

    public static class ActiveReducer extends Reducer<ActiveUserKey, IntWritable, ActiveUserKey, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(ActiveUserKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            List<Integer> userList = new ArrayList<Integer>(1000);
            int userId = 0;
            for(IntWritable val : values) {
                userId = val.get();
                if(!userList.contains(userId)) {
                    userList.add(userId);
                }
            }
            result.set(userList.size());
            context.write(key, result);
        }
    }

    public static class ActivePartitioner extends Partitioner<ActiveUserKey, IntWritable> {

        @Override
        public int getPartition(ActiveUserKey activeUserKey, IntWritable intWritable, int numberOfReducer) {
            //根据AppId和TerminalCode的和,与Reducer的数量取模
            return (activeUserKey.getAppId().get() + activeUserKey.getTerminalCode().get()) % numberOfReducer;
        }
    }
}
