package com.huayu.metis.io;

import com.huayu.metis.entry.LoginLogEntry;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.io.IOException;

/**
 * 登陆日志的输入格式
 * Created by Administrator on 14-7-12.
 */
public class LoginLogInputFormat extends SequenceFileInputFormat<LongWritable, LoginLogEntry> {

    @Override
    public RecordReader<LongWritable, LoginLogEntry> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return super.createRecordReader(split, context);
    }
}
