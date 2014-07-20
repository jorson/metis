package com.huayu.metis.flume.sink;

import com.huayu.metis.flume.sink.hdfs.HDFSSequenceFile;
import org.apache.flume.Event;

import java.io.IOException;

/**
 * Created by Administrator on 14-4-18.
 */
public class HdfsSeqWriterTest extends HDFSSequenceFile {
    protected volatile boolean closed, opened;

    private int openCount = 0;
    HdfsSeqWriterTest(int openCount) {
        this.openCount = openCount;
    }

    @Override
    public void open(String filePath) throws IOException {
        super.open(filePath);
        if(closed) {
            opened = true;
        }
    }

    @Override
    public void append(Event e) throws IOException {

        if (e.getHeaders().containsKey("fault")) {
            throw new IOException("Injected fault");
        } else if (e.getHeaders().containsKey("fault-once")) {
            e.getHeaders().remove("fault-once");
            throw new IOException("Injected fault");
        } else if (e.getHeaders().containsKey("fault-until-reopen")) {
            // opening first time.
            if(openCount == 1) {
                throw new IOException("Injected fault-until-reopen");
            }
        } else if (e.getHeaders().containsKey("slow")) {
            long waitTime = Long.parseLong(e.getHeaders().get("slow"));
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException eT) {
                throw new IOException("append interrupted", eT);
            }
        }

        super.append(e);
    }

    @Override
    public void close() throws IOException {
        closed = true;
        super.close();
    }
}
