package com.huayu.metis.rw;

import com.huayu.metis.entry.BaseLogEntry;
import com.huayu.metis.entry.FileOffset;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import java.io.IOException;

/**
 * 针对多个SequenceFile的记录读取者, 可以适应不同类型的记录实体
 * Created by Administrator on 14-7-12.
 */
public class MultiFileRecordReader<TEntry extends BaseLogEntry>
        extends SequenceFileRecordReader<FileOffset, TEntry> {

    private CombineFileSplit split;
    private long offset;
    private int count = 0;
    private FileOffset key;
    private TEntry value;
    private boolean hasInit = false;

    private Path[] paths;
    //当前SequenceFile的Reader对象
    private SequenceFile.Reader currentReader;
    private FileSystem hdfs;
    private long totalLength;

    public MultiFileRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException{
        initialize(split, context);
    }

    @Override
    public float getProgress() throws IOException {
        return super.getProgress();
    }

    @Override
    public FileOffset getCurrentKey() {
        return this.key;
    }

    @Override
    public TEntry getCurrentValue() {
        return this.value;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        if(this.hasInit) {
            return;
        }

        this.split = (CombineFileSplit)split;
        Configuration conf = context.getConfiguration();
        this.hdfs = FileSystem.get(conf);
        this.paths = this.split.getPaths();
        this.totalLength = this.split.getLength();

        Path file = this.paths[this.count];
        SequenceFile.Reader.Option optPath = SequenceFile.Reader.file(file);
        this.currentReader = new SequenceFile.Reader(conf, optPath);
        if(!this.hasInit) {
            this.hasInit = true;
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(count >= split.getNumPaths()) {
            return false;
        }
        if(key == null) {
            key = new FileOffset();
            key.setFilename(paths[count].toString());
        }
        //SequenceFile中日志格式为"Key=Int, Value=Text"
        LongWritable lineNumber = new LongWritable(0);
        Text lineContent = new Text("");
        boolean hasNext = false;

        do{
            hasNext = currentReader.next(lineNumber, lineContent);
            if(!hasNext) {
                //关闭当前的Reader
                this.currentReader.close();
                this.offset = split.getOffset(count);

                //如果已经没有更多的文件需要被读取
                if(++count >= split.getNumPaths()) {
                    return false;
                }
                //如果还有其他文件可以被读取, 则读取下一个文件
                Path file = this.paths[count];
                this.currentReader = new SequenceFile.Reader(this.hdfs.getConf(), SequenceFile.Reader.file(file));
                //设置结果Key的文件名称为当前文件名
                this.key.setFilename(file.getName());
            }
        } while(!hasNext);

        //读取到数据了, 设置当前Key中LineNumber字段
        this.key.setLineNumber(lineNumber.get());
        this.key.setOffset(this.currentReader.getPosition());
        return true;
    }
}
