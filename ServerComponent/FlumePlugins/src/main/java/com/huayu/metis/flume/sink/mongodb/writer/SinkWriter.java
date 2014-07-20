package com.huayu.metis.flume.sink.mongodb.writer;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;

import java.io.IOException;

/**
 * Created by Administrator on 14-4-10.
 */
public interface SinkWriter extends Configurable {

    public void open(String filePath) throws IOException;

    public void append(Event e) throws IOException;

    public void sync() throws IOException;

    public void close() throws IOException;

    public boolean isUnderReplicated();
}
