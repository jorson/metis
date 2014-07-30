package com.huayu.metis.mr.usage;

import com.huayu.metis.entry.VisitLogEntry;
import com.huayu.metis.keyvalue.usage.*;
import javafx.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.*;

/**
 * 用于计算页面访问频率的MR
 * Created by Administrator on 14-7-22.
 */
public class PageUseRateMapReduce {

    /**
     * 用户使用应用中的页面的Mapper
     */
    public static class UserUsePageMapper extends Mapper<LongWritable, VisitLogEntry, UserUseKey, IntWritable> {


        private UserUseKey writableKey = new UserUseKey();
        private IntWritable writableValue = new IntWritable(1);

        //将访问日志转换为使用频率的Key,并将访问的URL作为Value进行传递
        @Override
        protected void map(LongWritable key, VisitLogEntry value, Context context)
                throws IOException, InterruptedException {
            //如果用户的ID小于等于0, 直接忽略掉
            if(value.getUserId() <= 0){
                return;
            }

            //去掉注册日期中的时间部分
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(value.getVisitTime());
            cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DATE), 0, 0, 0);
            cal.set(Calendar.MILLISECOND, 0);

            //设置键中的各个字段信息
            writableKey.setStartDate(cal.getTimeInMillis());
            writableKey.setEndDate(cal.getTimeInMillis());
            writableKey.setAppId(value.getAppId());
            writableKey.setTerminalCode(value.getTerminalCode());
            writableKey.setUserId(value.getUserId());

            context.write(writableKey, writableValue);
        }
    }

    public static class UserUsePageReducer extends Reducer<UserUseKey, IntWritable, UserUseKey, IntWritable> {

        @Override
        protected void reduce(UserUseKey key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int result = 0;
            for(IntWritable val : values) {
                result += val.get();
            }
            if(result > 0) {
                context.write(key, new IntWritable(result));
            }
        }
    }

    public static class UserUsePageRateMapper extends Mapper<UserUseKey, IntWritable, UserUseRateKey, LongWritable> {

        @Override
        protected void map(UserUseKey key, IntWritable value, Context context) throws IOException, InterruptedException {
            UserUseRateKey writableKey = null;

            //按日计算
            if(key.getPeriodType().get() == 0) {
                writableKey = dayUseRate(key, value.get());
            }
            //按周计算
            else if (key.getPeriodType().get() == 1){
                writableKey = weekUseRate(key, value.get());
            }
            //按月计算
            else if (key.getPeriodType().get() == 2){
                writableKey = monthUseRate(key, value.get());
            }

            if(writableKey == null) {
                return;
            }

            context.write(writableKey, new LongWritable(key.getUserId().get()));
        }


        private UserUseRateKey dayUseRate(UserUseKey key, int userVisitTimes) {

            UserUseRateKey outKey = new UserUseRateKey();
            outKey.setStartDate(key.getStartDate());
            outKey.setEndDate(key.getEndDate());
            outKey.setAppId(key.getAppId());
            outKey.setPeriodType(key.getPeriodType());
            outKey.setTerminalCode(key.getTerminalCode());

            if(userVisitTimes >=1 && userVisitTimes <= 2) {
                outKey.setNormItemKey(100);
            } else if(userVisitTimes >=3 && userVisitTimes <= 5) {
                outKey.setNormItemKey(101);
            } else if(userVisitTimes >=6 && userVisitTimes <= 9) {
                outKey.setNormItemKey(102);
            } else if(userVisitTimes >=10 && userVisitTimes <= 29) {
                outKey.setNormItemKey(103);
            } else if(userVisitTimes >=30 && userVisitTimes <= 49) {
                outKey.setNormItemKey(104);
            } else if(userVisitTimes >=50) {
                outKey.setNormItemKey(105);
            } else {
                return null;
            }
            return outKey;
        }

        private UserUseRateKey weekUseRate(UserUseKey key, int userVisitTimes) {
            UserUseRateKey outKey = new UserUseRateKey();
            outKey.setStartDate(key.getStartDate());
            outKey.setEndDate(key.getEndDate());
            outKey.setAppId(key.getAppId());
            outKey.setPeriodType(key.getPeriodType());
            outKey.setTerminalCode(key.getTerminalCode());

            if(userVisitTimes >=1 && userVisitTimes <= 2) {
                outKey.setNormItemKey(100);
            } else if(userVisitTimes >=3 && userVisitTimes <= 5) {
                outKey.setNormItemKey(101);
            } else if(userVisitTimes >=6 && userVisitTimes <= 9) {
                outKey.setNormItemKey(102);
            } else if(userVisitTimes >=10 && userVisitTimes <= 29) {
                outKey.setNormItemKey(103);
            } else if(userVisitTimes >=30 && userVisitTimes <= 49) {
                outKey.setNormItemKey(104);
            } else if(userVisitTimes >=50) {
                outKey.setNormItemKey(105);
            } else {
                return null;
            }
            return outKey;
        }

        private UserUseRateKey monthUseRate(UserUseKey key, int userVisitTimes) {
            UserUseRateKey outKey = new UserUseRateKey();
            outKey.setStartDate(key.getStartDate());
            outKey.setEndDate(key.getEndDate());
            outKey.setAppId(key.getAppId());
            outKey.setPeriodType(key.getPeriodType());
            outKey.setTerminalCode(key.getTerminalCode());

            if(userVisitTimes >=1 && userVisitTimes <= 2) {
                outKey.setNormItemKey(100);
            } else if(userVisitTimes >=3 && userVisitTimes <= 5) {
                outKey.setNormItemKey(101);
            } else if(userVisitTimes >=6 && userVisitTimes <= 9) {
                outKey.setNormItemKey(102);
            } else if(userVisitTimes >=10 && userVisitTimes <= 29) {
                outKey.setNormItemKey(103);
            } else if(userVisitTimes >=30 && userVisitTimes <= 49) {
                outKey.setNormItemKey(104);
            } else if(userVisitTimes >=50) {
                outKey.setNormItemKey(105);
            } else {
                return null;
            }
            return outKey;
        }
    }

    public static class UserUsePageRateReducer extends Reducer<UserUseRateKey, LongWritable, UserUseRateKey, UserUseRateAmount> {

        @Override
        protected void reduce(UserUseRateKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            //输出的Key
            PageVisitOutputValue outputValue = new PageVisitOutputValue();
            //记录不重复的用户数量
            List<Long> userList = new ArrayList<Long>(1000);
            long userId = 0L;

            for(LongWritable val : values) {
                userId = val.get();
                if(!userList.contains(userId)) {
                    userList.add(userId);
                }
            }
            context.write(key, new UserUseRateAmount(userList.size()));
        }
    }


}
