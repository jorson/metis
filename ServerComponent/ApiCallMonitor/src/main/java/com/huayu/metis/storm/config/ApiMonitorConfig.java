package com.huayu.metis.storm.config;

import backtype.storm.Config;
import com.google.common.base.Preconditions;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

/**
 * Created by Administrator on 14-5-5.
 */
public class ApiMonitorConfig {

    private static Logger logger = LoggerFactory.getLogger(ApiMonitorConfig.class);

    /*与数据源有关的设置*/
    //TO DOC: 目前只支持MongoDb
    public static final String SOURCE_TYPE = "api.monitor.source.type";
    //数据源URL
    public static final String SOURCE_URL = "api.monitor.source.url";
    //数据源名称,如果是Mongo的话,表示源DbName
    public static final String SOURCE_CATALOG= "api.monitor.source.catalog";
    //数据源名称,如果是Mongo的话,表示源Collection
    public static final String SOURCE_NAME= "api.monitor.source.name";
    //界面
    public static final String SOURCE_TIME_STAMP_FIELD = "api.monitor.source.timestamp";

    //数据源是否需要验证
    public static final String SOURCE_NEED_AUTH = "api.monitor.source.needAuth";
    //数据源验证的用户名
    public static final String SOURCE_AUTH_USER = "api.monitor.source.auth.user";
    //数据源验证的秘密
    public static final String SOURCE_AUTH_PASSWORD = "api.monitor.source.auth.password";
    //按时间进行分组的间隔
    public static final String SOURCE_GROUP_INTERVAL = "api.monitor.source.group.interval";

    /*与目标有关的设置*/
    //TODO:目前只能支持MySQL, 之后要能够支持更多的目标类型
    public static final String TARGET_TYPE = "api.monitor.target.type";
    //目标的地址
    public static final String TARGET_URL = "api.monitor.target.url";
    //目标的名称, 如果是M有SQL的话,表示数据表名称
    public static final String TARGET_NAME = "api.monitor.target.name";
    //目标是否需要验证
    public static final String TARGET_NEED_AUTH = "api.monitor.target.needAuth";
    //目标验证的用户名
    public static final String TARGET_AUTH_USER = "api.monitor.target.auth.user";
    //目标验证的秘密
    public static final String TARGET_AUTH_PASSWORD = "api.monitor.target.auth.password";

    /*其他的配置*/
    public static final String DEBUG_MODE = "api.monitor.debug";
    public static final String BASE_TIMESTAMP_FILE_PATH = "api.monitor.base.timestamp.file";

    private String filePath;
    private Properties properties;
    private final Config config;

    public ApiMonitorConfig(String confiFilePath) {
        this.properties = new Properties();
        this.filePath = confiFilePath;
        this.config = new Config();
    }

    public Config build() throws Exception {
        File file = new File(filePath);
        if(!file.exists() || !file.isFile()) {
            throw new IOException("config file " + filePath + " not exists or not file");
        }
        InputStream is = new FileInputStream(file);
        properties.load(is);

        //数据源的设置
        String srcType = Preconditions.checkNotNull(properties.getProperty(SOURCE_TYPE),
                "api.monitor.source.type is REQUIRE");
        String srcUrl = Preconditions.checkNotNull(properties.getProperty(SOURCE_URL),
                "api.monitor.source.url is REQUIRE");
        String srcName = Preconditions.checkNotNull(properties.getProperty(SOURCE_NAME),
                "api.monitor.source.name is REQUIRE");
        String srcCatalog = Preconditions.checkNotNull(properties.getProperty(SOURCE_CATALOG),
                "api.monitor.source.catalog is REQUIRE");
        String groupInterval = Preconditions.checkNotNull(properties.getProperty(SOURCE_GROUP_INTERVAL),
                "api.monitor.source.group.interval is REQUIRE");
        String timestampPath = Preconditions.checkNotNull(properties.getProperty(SOURCE_TIME_STAMP_FIELD),
                "api.monitor.source.timestamp is REQUIRE");

        this.config.put(SOURCE_TYPE, srcType);
        this.config.put(SOURCE_URL, srcUrl);
        this.config.put(SOURCE_NAME, srcName);
        this.config.put(SOURCE_CATALOG, srcCatalog);
        this.config.put(SOURCE_GROUP_INTERVAL, groupInterval);
        this.config.put(SOURCE_TIME_STAMP_FIELD, timestampPath);

        String needAuth = properties.getProperty(SOURCE_NEED_AUTH, "false");
        //如果不需要验证
        if(needAuth.equalsIgnoreCase("false")){
            this.config.put(SOURCE_NEED_AUTH, false);
        } else {
            this.config.put(SOURCE_NEED_AUTH, true);
            String userName = Preconditions.checkNotNull(properties.getProperty(SOURCE_AUTH_USER),
                    "api.monitor.source.auth.user is REQUIRE");
            String password = Preconditions.checkNotNull(properties.getProperty(SOURCE_AUTH_PASSWORD),
                    "api.monitor.source.auth.password is REQUIRE");
            this.config.put(SOURCE_AUTH_USER, userName);
            this.config.put(SOURCE_AUTH_PASSWORD, password);
        }

        //目标的设置
        String dstType = Preconditions.checkNotNull(properties.getProperty(TARGET_TYPE),
                "api.monitor.target.type is REQUIRE");
        String dstUrl = Preconditions.checkNotNull(properties.getProperty(TARGET_URL),
                "api.monitor.target.url is REQUIRE");
        String dstName = Preconditions.checkNotNull(properties.getProperty(TARGET_NAME),
                "api.monitor.target.name is REQUIRE");

        this.config.put(TARGET_TYPE, dstType);
        this.config.put(TARGET_URL, dstUrl);
        this.config.put(TARGET_NAME, dstName);

        String dstNeedAuth = properties.getProperty(TARGET_NEED_AUTH, "true");
        //如果不需要验证
        if(dstNeedAuth.equalsIgnoreCase("false")){
            this.config.put(TARGET_NEED_AUTH, false);
        } else {
            this.config.put(TARGET_NEED_AUTH, true);
            String userName = Preconditions.checkNotNull(properties.getProperty(TARGET_AUTH_USER),
                    "api.monitor.target.auth.user is REQUIRE");
            String password = Preconditions.checkNotNull(properties.getProperty(TARGET_AUTH_PASSWORD),
                    "api.monitor.target.auth.password is REQUIRE");
            this.config.put(TARGET_AUTH_USER, userName);
            this.config.put(TARGET_AUTH_PASSWORD, password);
        }

        /*其他的设置*/
        String debugMode = properties.getProperty(DEBUG_MODE, "false");
        if(debugMode.equalsIgnoreCase("true")) {
            this.config.setDebug(true);
        }

        String timestampFile = Preconditions.checkNotNull(properties.getProperty(BASE_TIMESTAMP_FILE_PATH),
                "api.monitor.base.timestamp.file is REQUIRE");
        this.config.put(BASE_TIMESTAMP_FILE_PATH, timestampFile);
        return this.config;
    }

    /**
     * 从基准时间配置文件中读取最后的时间记录
     * @param path 时间配置文件
     * @return 最后时间记录
     * @throws Exception 各种异常
     */
    public static long getBaseTimestamp(String path) throws Exception {
        long result = 0L;
        File file = new File(path);
        if(!file.exists()) {
            //如果文件不存在,返回当前时间
            result = DateTime.now().getMillis();
            //setBaseTimestamp(result, path);
        }
        //如果文件是存在的
        else {
            BufferedReader br = null;
            String recordTimestamp = null;
            try {
                br = new BufferedReader(new FileReader(file));
                recordTimestamp = br.readLine();
            } catch (Exception ex) {
                logger.error("timestamp file read fail", ex);
            } finally {
                if(br != null) {
                    br.close();
                }
            }

            //如果读取不到数据
            if(recordTimestamp == null) {
                result = DateTime.now().getMillis();
                setBaseTimestamp(result, path);
            } else{
                //转换
                try {
                    result = Long.parseLong(recordTimestamp);
                //出现转换异常
                } catch (NumberFormatException e) {
                    logger.error("parse timestamp occurs error", e);
                    result = DateTime.now().getMillis();
                    setBaseTimestamp(result, path);
                }
            }
        }
        return result;
    }

    /**
     * 将最近的时间记录写入文件
     * @param timestamp 最近的时间文件
     * @param path 时间记录文件
     */
    public static void setBaseTimestamp(long timestamp, String path) throws Exception {
        File file = new File(path);
        if(!file.exists()) {
            file.createNewFile();
        }

        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(file));
            bw.write(String.valueOf(timestamp));
            bw.newLine();
            bw.flush();
        } catch(Exception ex){

        } finally {
            if(bw != null) {
                bw.close();
            }
        }
    }
}
