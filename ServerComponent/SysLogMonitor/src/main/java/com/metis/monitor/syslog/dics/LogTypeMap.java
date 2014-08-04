package com.metis.monitor.syslog.dics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Administrator on 14-8-4.
 */
public class LogTypeMap {

    private volatile static LogTypeMap instance = null;

    public static LogTypeMap getInstance() {
        if(instance == null) {
            synchronized (LogTypeMap.class) {
                if(instance == null) {
                    instance = new LogTypeMap();
                }
            }
        }
        return instance;
    }

    private LogTypeMap() {

    }

    private void init() {

    }

    public int search(String key) {
        return 0;
    }

    public void add(String key, int value) {

    }

    public void flush() {

    }
}
