package com.huayu.metis.mr.trend;

import com.huayu.metis.entry.RegisterLogEntry;
import com.huayu.metis.entry.VisitLogEntry;
import com.huayu.metis.keyvalue.trend.AddedUserKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * 用于每天新增用户的统计
 * Created by Administrator on 14-7-21.
 */
public class AddedMapReduce {

    /**
     * 注册用户的Mapper
     */
    public static class RegisterMapper extends Mapper<LongWritable, RegisterLogEntry, AddedUserKey, IntWritable> {

        private AddedUserKey writableKey = new AddedUserKey();
        private IntWritable writableValue = new IntWritable(0);

        @Override
        protected void map(LongWritable key, RegisterLogEntry value, Context context) throws IOException, InterruptedException {
            //去掉注册日期中的时间部分
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(value.getRegisterTime());
            GregorianCalendar gCal = new GregorianCalendar(cal.get(Calendar.YEAR),
                    cal.get(Calendar.MONTH) + 1,
                    cal.get(Calendar.DATE));

            //设置键中的各个字段信息
            writableKey.setStartDate(new LongWritable(gCal.getTimeInMillis()));
            writableKey.setEndDate(new LongWritable(gCal.getTimeInMillis()));
            writableKey.setPeriodType(new IntWritable(1));
            writableKey.setAppId(new IntWritable(value.getAppId()));
            writableKey.setTerminalCode(new IntWritable(value.getTerminalCode()));
            //将用户ID设置为值信息
            writableValue.set(value.getUserId());

            context.write(writableKey, writableValue);
        }
    }

    /**
     * 访问用户的Mapper
     */
    public static class VisitUserMapper extends Mapper<VisitLogEntry, IntWritable, AddedUserKey, IntWritable> {

        @Override
        protected void map(VisitLogEntry key, IntWritable value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }
    }

    public static class AddedCombiner extends Reducer<AddedUserKey, IntWritable, AddedUserKey, List<IntWritable>> {

        @Override
        protected void reduce(AddedUserKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            List<IntWritable> userIdList = new ArrayList<IntWritable>(100);
            //做一次Combine
            for(IntWritable val : values) {
                if(!userIdList.contains(val)){
                    userIdList.add(val);
                }
            }
            context.write(key, userIdList);
        }
    }

    public static class AddedReducer extends Reducer<AddedUserKey, List<IntWritable>, AddedUserKey, IntWritable> {
        IntWritable writableValue = new IntWritable(0);

        @Override
        protected void reduce(AddedUserKey key, Iterable<List<IntWritable>> values, Context context) throws IOException, InterruptedException {
            List<IntWritable> combineUserList = combineAllList(values);
            List<IntWritable> distinctUserList = new ArrayList<IntWritable>(100);
            for(IntWritable val : combineUserList) {
                if(!distinctUserList.contains(val)) {
                    distinctUserList.add(val);
                }
            }
            writableValue.set(distinctUserList.size());
            context.write(key, writableValue);
        }

        private List<IntWritable> combineAllList(Iterable<List<IntWritable>> values) {
            List<IntWritable> combineList = new ArrayList<IntWritable>(100);
            for(List<IntWritable> val : values) {
                combineList.addAll(val);
            }
            return combineList;
        }
    }

    /**
     * 用户的分区配置
     */
    public static class AddedPartitioner extends Partitioner<AddedUserKey, IntWritable> {
        @Override
        public int getPartition(AddedUserKey addedUserKey, IntWritable intWritable, int num) {
            //根据用户的ID进行分组,用户ID与Reducer的数量取模,相同模数的用户ID被分配到一个分区中
            int userId = intWritable.get();
            return userId % num;
        }
    }
}
