package com.metis.monitor.syslog.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 系统日志的管理器
 * Created by jorson on 14-8-5.
 */
public class SysLogTypeManager {

    private static Logger logger = LoggerFactory.getLogger(SysLogTypeManager.class);
    //管理器单例
    private static volatile SysLogTypeManager instance;



    public static SysLogTypeManager getInstance() {
        synchronized (SysLogTypeManager.class) {
            if(instance == null) {
                synchronized (SysLogTypeManager.class) {
                    instance = new SysLogTypeManager();
                }
            }
        }
        return instance;
    }

    private SysLogTypeManager() {

    }

    private void initCache() {

    }

    private void initSourceDb() {

    }
}
