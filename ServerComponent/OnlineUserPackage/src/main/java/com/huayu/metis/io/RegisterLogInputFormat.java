package com.huayu.metis.io;

import com.huayu.metis.entry.RegisterLogEntry;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.io.IOException;

/**
 * 注册日志的输入格式
 * Created by Administrator on 14-7-12.
 */
public class RegisterLogInputFormat extends SequenceFileInputFormat<LongWritable, RegisterLogEntry> {

    @Override
    public RecordReader<LongWritable, RegisterLogEntry> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return super.createRecordReader(split, context);
    }
}
