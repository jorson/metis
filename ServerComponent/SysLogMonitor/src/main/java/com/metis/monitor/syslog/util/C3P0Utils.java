package com.metis.monitor.syslog.util;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.metis.monitor.syslog.config.SysLogConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by Administrator on 14-8-11.
 */
public class C3P0Utils {

    private static volatile C3P0Utils instance = null;
    private ComboPooledDataSource comboPooledDataSource = null;
    private static Logger logger = LoggerFactory.getLogger(C3P0Utils.class);
    private static boolean hasInit = false;

    public static C3P0Utils getInstance() {
        synchronized (C3P0Utils.class) {
            if(instance == null) {
                synchronized (C3P0Utils.class) {
                    instance = new C3P0Utils();
                }
            }
        }
        return instance;
    }

    public void init(String driver, String url, String user, String password) {
        System.out.println(String.format("[C3P0Utils]Connection Info:%s,%s,%s,%s",
                driver, url, user, password));
        if(hasInit) {
            return;
        }

        if(comboPooledDataSource == null) {
            comboPooledDataSource = new ComboPooledDataSource();
        }
        try {
            comboPooledDataSource.setDriverClass(driver);
        } catch (PropertyVetoException e) {
            if(logger.isErrorEnabled()) {
                logger.error("C3P0Utils", e);
            }
        }
        comboPooledDataSource.setUser(user);
        comboPooledDataSource.setPassword(password);
        comboPooledDataSource.setJdbcUrl(url);
        //C3P0的可选配置
//        comboPooledDataSource.setMinPoolSize(5);
//        comboPooledDataSource.setMaxPoolSize(20);
//        comboPooledDataSource.setMaxIdleTime(15);
        comboPooledDataSource.setMinPoolSize(30);
        comboPooledDataSource.setMaxPoolSize(120);
        comboPooledDataSource.setMaxIdleTime(1800);
        comboPooledDataSource.setUnreturnedConnectionTimeout(15);

        hasInit = true;
    }

    private C3P0Utils() {

    }

    public Connection getConnection() {
        Connection connection = null;
        try {
            connection = comboPooledDataSource.getConnection();
        } catch (SQLException ex) {
            if(logger.isErrorEnabled()) {
                logger.error("C3P0Utils", ex);
            }
        }
        return connection;
    }
}
