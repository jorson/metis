package com.huayu.metis.flume.utility;

import com.huayu.metis.flume.sink.mongodb.writer.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Administrator on 14-4-21.
 */
public class WriterLinkedHashMap<K, V extends SinkWriter> extends LinkedHashMap<K, V> {
    private final int maxOpenFiles;
    private static final Logger LOG = LoggerFactory.getLogger(WriterLinkedHashMap.class);

    public WriterLinkedHashMap(int maxOpenFiles){
        super(16, 0.75f, true);
        this.maxOpenFiles = maxOpenFiles;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest){
        if(size() > maxOpenFiles){
            //如果打开的文件数量已经超过设置的文件数量, 就把最后一个关闭掉, 并返回 true
            try{
                eldest.getValue().close();
            } catch (Throwable e) {
                LOG.warn(eldest.getKey().toString(), e);
                if(e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
            return true;
        } else{
            return false;
        }
    }
}
