package com.huayu.metis.mr.usage;

import com.huayu.metis.entry.VisitLogEntry;
import com.huayu.metis.keyvalue.usage.PageVisitValue;
import com.huayu.metis.keyvalue.usage.UseRateKey;
import com.huayu.metis.keyvalue.usage.UserUseKey;
import javafx.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

/**
 * 用于计算页面访问频率的MR
 * Created by Administrator on 14-7-22.
 */
public class PageUseRateMapReduce {

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
            GregorianCalendar gCal = new GregorianCalendar(cal.get(Calendar.YEAR),
                    cal.get(Calendar.MONTH),
                    cal.get(Calendar.DATE));

            //设置键中的各个字段信息
            writableKey.setVisitDate(gCal.getTimeInMillis());
            writableKey.setAppId(value.getAppId());
            writableKey.setTerminalCode(value.getTerminalCode());
            writableKey.setUserId(value.getUserId());
            writableKey.setPageUrl(value.getVisitPage());

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

    public static class UserUseMapper extends Mapper<UserUseKey, IntWritable, UserUseKey, PageVisitValue> {

        private UserUseKey writableKey = new UserUseKey();
        private PageVisitValue writableValue = new PageVisitValue();

        @Override
        protected void map(UserUseKey key, IntWritable value, Context context) throws IOException, InterruptedException {
            //将再输出的Key中的用户ID设置为0, 以便降维
            writableKey.setUserId(0);
            //将输入Key中的PageURL设置为相同值, 以便于在Reducer中进行降为维度
            writableKey.setPageUrl(new Text(""));
            writableKey.setAppId(key.getAppId().get());
            writableKey.setTerminalCode(key.getTerminalCode().get());
            writableKey.setVisitDate(key.getVisitDate().get());

            //将输入的Mapper和Key中UserId设置为输出
            writableValue.setVisitUserId(key.getUserId().get());
            //将访问次数设置为输出的Value
            writableValue.setVisitTimes(value.get());

            context.write(writableKey, writableValue);
        }
    }

    public static class UserUseReducer extends Reducer<UserUseKey, PageVisitValue, UseRateKey, PageVisitValue> {

        private MultipleOutputs<UseRateKey, IntWritable> multiOutput;


        @Override
        protected void reduce(UserUseKey key, Iterable<PageVisitValue> values, Context context)
                throws IOException, InterruptedException {

            //计算不重复的访问用户数和访问量
            HashMap<Long, Integer> userVisitMap = new HashMap<Long, Integer>(200);
            long userId = 0L;
            int visitTime = 0;
            for(PageVisitValue val : values) {
                userId = val.getVisitUserId().get();
                visitTime = val.getVisitTimes().get();
                //如果MAP中不存在对应的数据
                if(!userVisitMap.containsKey(userId)) {
                    userVisitMap.put(userId, visitTime);
                } else {
                    userVisitMap.put(userId, userVisitMap.get(userId) + visitTime);
                }
            }
            //所有的用户都被集合在一个HashMap中
            //循环Map中的对象
            for(Map.Entry<Long, Integer> entry : userVisitMap.entrySet()) {
                //构建输出结果的Key
                UseRateKey outputKey = new UseRateKey();
                outputKey.setAppId(key.getAppId().get());
                outputKey.setTerminalCode(key.getTerminalCode().get());
                outputKey.setVisitDate(key.getVisitDate().get());

                int visitTimes = entry.getValue().intValue();
                if(visitTimes>=1 && visitTimes<=2) {
                    outputKey.setNormItemKey(100);
                } else if(visitTimes>=3 && visitTimes<=5){
                    outputKey.setNormItemKey(101);
                } else if(visitTimes>=6 && visitTimes<=9){
                    outputKey.setNormItemKey(102);
                } else if(visitTimes>=10 && visitTimes<=29){
                    outputKey.setNormItemKey(103);
                } else if(visitTimes>=30 && visitTimes<=49){
                    outputKey.setNormItemKey(104);
                } else if(visitTimes>=50){
                    outputKey.setNormItemKey(105);
                }

                //multiOutput.write("visit_times", outputKey, );
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            multiOutput = new MultipleOutputs(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            if(multiOutput != null){
                multiOutput.close();
            }
        }
    }

}
