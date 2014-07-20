package com.huayu.metis.flume.sink;

import com.huayu.metis.flume.sink.hdfs.HDFSWriter;
import com.huayu.metis.flume.sink.hdfs.HDFSWriterFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Administrator on 14-4-18.
 */
public class HdfsWriterFactoryTest extends HDFSWriterFactory {

    static final String TestSequenceFileType = "SequenceFile";

    AtomicInteger openCount = new AtomicInteger(0);

    @Override
    public HDFSWriter getWriter(String fileType) throws IOException {

        if(fileType == TestSequenceFileType) {
            return new HdfsSeqWriterTest(openCount.incrementAndGet());
        } else {
            throw new IOException("File type " + fileType + " not supported");
        }
    }
}
