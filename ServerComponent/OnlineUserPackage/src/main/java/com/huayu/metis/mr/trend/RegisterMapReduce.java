package com.huayu.metis.mr.trend;

import com.huayu.metis.entry.RegisterLogEntry;
import com.huayu.metis.keyvalue.trend.RegisterUserKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * 计算当天注册用户量的MR
 * Created by Administrator on 14-7-14.
 */
public class RegisterMapReduce {

    private static final Logger logger = LoggerFactory.getLogger(RegisterMapReduce.class);

    public static class RegisterMapper extends Mapper<LongWritable, RegisterLogEntry, RegisterUserKey, IntWritable> {
        private RegisterUserKey writableKey = new RegisterUserKey();
        private IntWritable writableValue = new IntWritable(0);

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
            //设置用户的ID
            writableValue.set(value.getUserId());

            context.write(writableKey, writableValue);
        }


    }

    public static class RegisterReducer extends Reducer<RegisterUserKey, IntWritable, RegisterUserKey, IntWritable> {

        private IntWritable result = new IntWritable();
        private MultipleOutputs<RegisterUserKey, IntWritable> multipleOutput;

        private final String basePathArgs = "UserBasePath";

        @Override
        protected void reduce(RegisterUserKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values) {
                sum += 1;
                //将信息输出到特定的文件中
                multipleOutput.write("register_user", key, val, context.getConfiguration().get("UserBasePath"));
            }
            result.set(sum);
            context.write(key, result);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutput = new MultipleOutputs<RegisterUserKey, IntWritable>(context);
            super.setup(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if(multipleOutput != null){
                multipleOutput.close();
            }
            super.cleanup(context);
        }

        private String generateRegisterFileName(long currentDate, String basePath) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(currentDate);

            String targetPath = String.format("%s/%d-%d-%d/register_user", basePath,
                    calendar.get(Calendar.YEAR),
                    calendar.get(Calendar.MONTH),
                    calendar.get(Calendar.DATE));
            return targetPath;
        }
    }
}
