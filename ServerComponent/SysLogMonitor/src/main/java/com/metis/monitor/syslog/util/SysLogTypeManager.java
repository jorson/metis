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
    private Connection connection;

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
        int port = SysLogConfig.getInstance().tryGetInt(SysLogConfig.CACHE_PORT, 6378);
        int catalog = SysLogConfig.getInstance().tryGetInt(SysLogConfig.CACHE_CATALOG, 1);
        redisClient = RedisClient.getInstance(host, port, catalog);
    }

    private void initSourceDb() {
        String driver = SysLogConfig.getInstance().tryGet(SysLogConfig.TARGET_DRIVER);
        String url = SysLogConfig.getInstance().tryGet(SysLogConfig.TARGET_URL);
        String user = SysLogConfig.getInstance().tryGet(SysLogConfig.TARGET_USER);
        String password = SysLogConfig.getInstance().tryGet(SysLogConfig.TARGET_PASSWORD);

        try{
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            if(logger.isErrorEnabled()) {
                logger.error("Connect MySQL error", e);
            }
        }
    }

    public SysLogType get(String key) {
        String result = null;
        synchronized (SysLogTypeManager.class) {
            result = redisClient.get(key);
            if("null".equalsIgnoreCase(result)) {
                SysLogType item = missingHandler.handle(key);
                if(item == null) {
                    synchronized (SysLogTypeManager.class) {

                    }
                }
            }
        }
    }

    public interface SysLogTypeMissingHandler {
        public SysLogType handle(SysLogType entry);
    }
}
