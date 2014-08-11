package com.metis.monitor.syslog.util;

import com.metis.monitor.syslog.config.SysLogConfig;
import com.metis.monitor.syslog.entry.SysLogType;
import com.metis.monitor.syslog.util.redis.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * 系统日志的管理器
 * Created by jorson on 14-8-5.
 */
public class SysLogTypeManager {

    private static Logger logger = LoggerFactory.getLogger(SysLogTypeManager.class);
    //管理器单例
    private static volatile SysLogTypeManager instance;
    //未找到数据时的Handler
    private SysLogTypeMissingHandler missingHandler;

    private RedisClient redisClient;

    public static SysLogTypeManager getInstance(SysLogTypeMissingHandler missingHandler) {
        synchronized (SysLogTypeManager.class) {
            if(instance == null) {
                synchronized (SysLogTypeManager.class) {
                    instance = new SysLogTypeManager(missingHandler);
                }
            }
        }
        return instance;
    }

    private SysLogTypeManager(SysLogTypeMissingHandler missingHandler) {
        this.missingHandler = missingHandler;
        initCache();
    }

    private void initCache() {
        String host = SysLogConfig.getInstance().tryGet(SysLogConfig.CACHE_HOST);
        int port = SysLogConfig.getInstance().tryGetInt(SysLogConfig.CACHE_PORT, 6379);
        int catalog = SysLogConfig.getInstance().tryGetInt(SysLogConfig.CACHE_CATALOG, 1);
        redisClient = RedisClient.getInstance(host, port, catalog);
    }

    public Integer get(SysLogType entry) {
        String result = null;
        result = redisClient.get(entry.getFeatureCode());
        if(result == null) {
            synchronized (SysLogTypeManager.class) {
                SysLogType item = missingHandler.handle(entry);
                redisClient.set(item.getFeatureCode(), String.valueOf(item.getLogId()));
                return item.getLogId();
            }
        } else {
            return Integer.parseInt(result);
        }
    }

    public interface SysLogTypeMissingHandler {
        public SysLogType handle(SysLogType entry);
    }
}
