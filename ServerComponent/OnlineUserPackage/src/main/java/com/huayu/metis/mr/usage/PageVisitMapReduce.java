package com.huayu.metis.mr.usage;

import com.huayu.metis.entry.VisitLogEntry;
import com.huayu.metis.keyvalue.usage.PageVisitKey;
import com.huayu.metis.keyvalue.usage.PageVisitOutputValue;
import com.huayu.metis.keyvalue.usage.PageVisitValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * 页面访问情况的M/R包
 * Created by Administrator on 14-7-30.
 */
public class PageVisitMapReduce {

    public static class PageVisitMapper extends Mapper<LongWritable, VisitLogEntry, PageVisitKey, PageVisitValue> {

        private PageVisitKey writableKey = new PageVisitKey();
        private PageVisitValue writableValue = new PageVisitValue();

        @Override
        protected void map(LongWritable key, VisitLogEntry value, Context context)
                throws IOException, InterruptedException {
            //从配置中获取处理的类型
            String periodType = context.getConfiguration().get("custom.period", "none");
            //如果处理周期没有被设置
            if(periodType.equalsIgnoreCase("none")) {
                return;
            }

            Calendar inDate = Calendar.getInstance();
            inDate.setFirstDayOfWeek(Calendar.MONDAY);

            inDate.setTimeInMillis(value.getVisitTime());
            //设置输出的开始和结束
            if(periodType.equalsIgnoreCase("day")) {
                writableKey.setStartDate(inDate.getTimeInMillis());
                writableKey.setEndDate(inDate.getTimeInMillis());
                writableKey.setPeriodType(0);
            } else if(periodType.equalsIgnoreCase("week")) {
                Long[] startEnd = getWeekStartEnd(inDate);
                writableKey.setStartDate(startEnd[0]);
                writableKey.setEndDate(startEnd[1]);
                writableKey.setPeriodType(1);
            } else if(periodType.equalsIgnoreCase("month")) {
                Long[] startEnd = getMonthStartEnd(inDate);
                writableKey.setStartDate(startEnd[0]);
                writableKey.setEndDate(startEnd[1]);
                writableKey.setPeriodType(2);
            }
            writableKey.setAppId(value.getAppId());
            writableKey.setTerminalCode(value.getTerminalCode());
            writableKey.setVisitUrl(value.getVisitPage());

            writableValue.setVisitUserId(value.getUserId());
            writableValue.setVisitTimes(1);

            context.write(writableKey, writableValue);
        }

        private Long[] getWeekStartEnd(Calendar in) {
            Long[] results = new Long[2];
            Calendar calendar = Calendar.getInstance();
            calendar.set(in.get(Calendar.YEAR),
                    in.get(Calendar.MONTH),
                    in.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
            calendar.set(Calendar.MILLISECOND, 0);

            int weekDay = calendar.get(Calendar.DAY_OF_WEEK);
            if(weekDay == Calendar.SUNDAY) {
                calendar.add(Calendar.DATE, 6);
            } else {
                calendar.add(Calendar.DATE, 2 - weekDay);
            }

            results[0] = calendar.getTimeInMillis();
            calendar.add(Calendar.DATE, 6);
            results[1] = calendar.getTimeInMillis();
            return  results;
        }

        private Long[] getMonthStartEnd(Calendar in) {
            Long[] results = new Long[2];
            Calendar calendar = Calendar.getInstance();
            calendar.set(in.get(Calendar.YEAR),
                    in.get(Calendar.MONTH),
                    in.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
            calendar.set(Calendar.MILLISECOND, 0);

            calendar.set(Calendar.DATE, calendar.getActualMinimum(Calendar.DAY_OF_MONTH));
            results[0] = calendar.getTimeInMillis();

            calendar.set(Calendar.DATE, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
            results[1] = calendar.getTimeInMillis();

            return results;
        }
    }

    public static class PageVisitReducer extends Reducer<PageVisitKey, PageVisitValue, PageVisitKey, PageVisitOutputValue> {

        @Override
        protected void reduce(PageVisitKey key, Iterable<PageVisitValue> values, Context context) throws IOException, InterruptedException {
            //输出的Key
            PageVisitOutputValue outputValue = new PageVisitOutputValue();
            //记录不重复的用户数量
            List<Long> userList = new ArrayList<Long>(1000);
            long userId = 0L;
            int visitTimes = 0;

            for(PageVisitValue val : values) {
                userId = val.getVisitUserId().get();
                if(!userList.contains(userId)) {
                    userList.add(userId);
                }
                visitTimes += val.getVisitTimes().get();
            }
            outputValue.setVisitUsers(new IntWritable(userList.size()));
            outputValue.setVisitTimes(new IntWritable(visitTimes));

            context.write(key, outputValue);
        }
    }

    public static class PageVisitPartitioner extends Partitioner<PageVisitKey, PageVisitValue> {

        //根据APP_ID进行分区
        @Override
        public int getPartition(PageVisitKey pageVisitKey, PageVisitValue pageVisitValue, int taskNum) {
            return pageVisitKey.getAppId().get() % taskNum;
        }
    }
}
