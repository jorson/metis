package com.huayu.metis.config;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Administrator on 14-7-28.
 */
public class UserOnlinePackageConfig extends BaseConfig {
    //原始文件的路径
    public static final String ORIGINAL_PATH = "user.online.packing.original.path";
    //合并文件的路径
    public static final String COMBINE_PATH = "user.online.packing.combine.path";
    //文件合并后是否删除原始文件
    public static final String DELETE_ORIGINAL = "user.online.packing.delete.original";
    //控制数据库的URL
    public static final String CONTROL_URL = "user.online.packing.control.url";
    //控制数据库的名称
    public static final String CONTROL_CATALOG = "user.online.packing.control.catalog";
    //控制数据库的集合名词
    public static final String CONTROL_NAME = "user.online.packing.control.name";

    private static boolean hasLoaded = false;

    private static UserOnlinePackageConfig config;

    public static UserOnlinePackageConfig getInstance() {
        if(config == null) {
            config = new UserOnlinePackageConfig();
        }
        return config;
    }

    public UserOnlinePackageConfig() {
        super();
    }

    @Override
    public void loadConfig(String filePath) throws Exception {

        if(hasLoaded) {
            return;
        }

        File file = new File(filePath);
        if(!file.exists() || !file.isFile()) {
            throw new IOException("config file " + filePath + " not exists or not file");
        }
        InputStream is = new FileInputStream(file);
        this.properties.load(is);

        //添加配置
        this.trySet(ORIGINAL_PATH, true, "");
        this.trySet(COMBINE_PATH, true, "");
        this.trySet(DELETE_ORIGINAL, false, "true");
        this.trySet(CONTROL_URL, true, "");
        this.trySet(CONTROL_CATALOG, true, "");
        this.trySet(CONTROL_NAME, true, "");

        hasLoaded = true;
    }
}
