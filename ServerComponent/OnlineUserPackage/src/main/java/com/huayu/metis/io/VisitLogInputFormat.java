package com.huayu.metis.io;

import com.huayu.metis.entry.VisitLogEntry;
import com.huayu.metis.rw.CombineSequenceFileRecordReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.io.IOException;

/**
 * 页面浏览日志的输入格式
 * Created by Administrator on 14-7-12.
 */
public class VisitLogInputFormat extends SequenceFileInputFormat<LongWritable, VisitLogEntry> {
    @Override
    public RecordReader<LongWritable, VisitLogEntry> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        CombineSequenceFileRecordReader reader = new CombineSequenceFileRecordReader<VisitLogEntry>();
        return reader;
    }
}
