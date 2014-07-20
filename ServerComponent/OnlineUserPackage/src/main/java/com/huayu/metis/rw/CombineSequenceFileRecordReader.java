package com.huayu.metis.rw;

import com.huayu.metis.entry.BaseLogEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import java.io.IOException;

/**
 * Created by Administrator on 14-7-18.
 */
public class CombineSequenceFileRecordReader<TEntry extends BaseLogEntry>
        extends SequenceFileRecordReader<LongWritable, TEntry> {

    private LongWritable key;
    private TEntry value;

    public CombineSequenceFileRecordReader() {
        super();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return super.nextKeyValue();
    }

    @Override
    public LongWritable getCurrentKey() {
        return this.key;
    }

    @Override
    public TEntry getCurrentValue() {
        return this.value;
    }
}
