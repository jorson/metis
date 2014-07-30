package com.huayu.metis.config;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Properties;

/**
 * Created by Administrator on 14-7-28.
 */
abstract class BaseConfig extends HashMap<String, Object> {

    protected Properties properties;

    public BaseConfig() {
        this.properties = new Properties();
    }

    public BaseConfig(Properties properties) {
        if(properties == null)
            throw new NullPointerException("properties can't NULL");
        this.properties = properties;
    }

    public abstract void loadConfig(String filePath) throws Exception;

    public String tryGet(String key) {
        Object obj = this.get(key);
        if(obj == null) {
            return "";
        }
        return obj.toString();
    }

    public String tryGet(String key, String defaultValue) {
        Object obj = this.get(key);
        if(obj == null) {
            return defaultValue;
        }
        return obj.toString();
    }

    public long tryGetLong(String key, long defaultValue) {
        Object obj = this.get(key);
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
        Object obj = this.get(key);
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
        Object obj = this.get(key);
        if(obj == null) {
            return defaultValue;
        }
        try {
            return Boolean.parseBoolean(obj.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 尝试从Properties中获取指定的Key, 并设置到配置对象中
     * @param key 指定Key
     * @param isRequire 是否必须
     * @param defaultValue 默认值
     */
    protected void trySet(String key, boolean isRequire, String defaultValue) {
        String keyValue;
        if(isRequire) {
            keyValue = Preconditions.checkNotNull(properties.getProperty(key),
                    String.format("%s is REQUIRE", key));
        } else {
            keyValue = properties.getProperty(key, defaultValue);
        }
        this.put(key, keyValue);
    }
}
