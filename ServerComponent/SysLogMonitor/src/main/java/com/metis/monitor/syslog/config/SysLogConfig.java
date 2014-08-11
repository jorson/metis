package com.metis.monitor.syslog.config;

import backtype.storm.Config;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Administrator on 14-8-4.
 */
public class SysLogConfig {

    private static Logger logger = LoggerFactory.getLogger(SysLogConfig.class);

    //目标数据源的配置
    public static final String TARGET_DRIVER = "metis.syslog.target.driver";
    public static final String TARGET_URL = "metis.syslog.target.url";
    public static final String TARGET_USER = "metis.syslog.target.user";
    public static final String TARGET_PASSWORD = "metis.syslog.target.password";
    //控制源的配置
    public static final String CONTROL_URL = "metis.syslog.control.url";
    public static final String CONTROL_CATALOG = "metis.syslog.control.catalog";
    public static final String CONTROL_NAME = "metis.syslog.control.name";
    //组件配置
    public static final String ZOOKEEPER_HOSTS = "metis.syslog.kafka.zookeeper.hosts";
    public static final String KAFKA_TOPIC = "metis.syslog.kafka.topic";
    public static final String MIN_INTERVAL_MS = "metis.syslog.min.interval.ms";
    //调试开关
    public static final String DEBUG_MODE = "metis.syslog.debug";
    //缓存配置
    public static final String CACHE_HOST = "metis.syslog.cache.host";
    public static final String CACHE_PORT = "metis.syslog.cache.port";
    public static final String CACHE_CATALOG = "metis.syslog.cache.catalog";

    private Properties properties;
    private final Config config;
    private static boolean hasLoaded = false;

    private static SysLogConfig sysLogConfig;

    public static SysLogConfig getInstance() {
        if(sysLogConfig == null) {
            sysLogConfig = new SysLogConfig();
        }
        return sysLogConfig;
    }

    public SysLogConfig() {
        this.properties = new Properties();
        this.config = new Config();
    }

    public Config loadConfig(String configFilePath) throws Exception {
        if(hasLoaded) {
            return this.config;
        }

        File file = new File(configFilePath);
        if(!file.exists() || !file.isFile()) {
            throw new IOException("config file " + configFilePath + " not exists or not file");
        }

        InputStream is = new FileInputStream(file);
        properties.load(is);

        String targetDriver = Preconditions.checkNotNull(properties.getProperty(TARGET_DRIVER),
                TARGET_DRIVER + " is REQUIRE");
        String targetUrl = Preconditions.checkNotNull(properties.getProperty(TARGET_URL),
                TARGET_URL + " is REQUIRE");
        String targetUser = Preconditions.checkNotNull(properties.getProperty(TARGET_USER),
                TARGET_USER + " is REQUIRE");
        String targetPassword = Preconditions.checkNotNull(properties.getProperty(TARGET_PASSWORD),
                TARGET_PASSWORD + " is REQUIRE");

        String controlUrl = Preconditions.checkNotNull(properties.getProperty(CONTROL_URL),
                CONTROL_URL + " is REQUIRE");
        String controlCatalog = Preconditions.checkNotNull(properties.getProperty(CONTROL_CATALOG),
                CONTROL_CATALOG + " is REQUIRE");
        String controlName = Preconditions.checkNotNull(properties.getProperty(CONTROL_NAME),
                CONTROL_NAME + " is REQUIRE");

        String zookeeperHosts = Preconditions.checkNotNull(properties.getProperty(ZOOKEEPER_HOSTS),
                ZOOKEEPER_HOSTS + " is REQUIRE");
        String kafkaTopic = Preconditions.checkNotNull(properties.getProperty(KAFKA_TOPIC),
                KAFKA_TOPIC + " is REQUIRE");
        String minIntervalMs = Preconditions.checkNotNull(properties.getProperty(MIN_INTERVAL_MS),
                MIN_INTERVAL_MS + " is REQUIRE");

        String cacheHost = Preconditions.checkNotNull(properties.getProperty(CACHE_HOST),
                CACHE_HOST + " is REQUIRE");
        String cachePort = Preconditions.checkNotNull(properties.getProperty(CACHE_PORT),
                CACHE_PORT + " is REQUIRE");
        String cacheCatalog = Preconditions.checkNotNull(properties.getProperty(CACHE_CATALOG),
                CACHE_CATALOG + " is REQUIRE");

        this.config.put(TARGET_DRIVER, targetDriver);
        this.config.put(TARGET_URL, targetUrl);
        this.config.put(TARGET_USER, targetUser);
        this.config.put(TARGET_PASSWORD, targetPassword);

        this.config.put(CONTROL_URL, controlUrl);
        this.config.put(CONTROL_CATALOG, controlCatalog);
        this.config.put(CONTROL_NAME, controlName);

        this.config.put(ZOOKEEPER_HOSTS, zookeeperHosts);
        this.config.put(KAFKA_TOPIC, kafkaTopic);
        this.config.put(MIN_INTERVAL_MS, minIntervalMs);

        this.config.put(CACHE_HOST, cacheHost);
        this.config.put(CACHE_PORT, cachePort);
        this.config.put(CACHE_CATALOG, cacheCatalog);

        logger.info("SysLogConfig", String.format("%s,%s,%s,%s", targetDriver, targetUrl, targetUser, targetPassword));

        /*其他的设置*/
        String debugMode = properties.getProperty(DEBUG_MODE, "false");
        if(debugMode.equalsIgnoreCase("true")) {
            this.config.setDebug(true);
        }

        hasLoaded = true;
        return this.config;
    }

    public String tryGet(String key) {
        Object obj = this.config.get(key);
        if(obj == null) {
            return "";
        }
        return obj.toString();
    }

    public String tryGet(String key, String defaultValue) {
        Object obj = this.config.get(key);
        if(obj == null) {
            return defaultValue;
        }
        return obj.toString();
    }

    public long tryGetLong(String key, long defaultValue) {
        Object obj = this.config.get(key);
        if(obj == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(obj.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public int tryGetInt(String key, int defaultValue) {
        Object obj = this.config.get(key);
        if(obj == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(obj.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public boolean tryGetBoolean(String key, boolean defaultValue) {
        Object obj = this.config.get(key);
        if(obj == null) {
            return defaultValue;
        }
        try {
            return Boolean.parseBoolean(obj.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
