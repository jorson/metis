package com.metis.monitor.syslog.util;

import com.metis.monitor.syslog.config.SysLogConfig;
import com.metis.monitor.syslog.entry.SysLogType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * Created by Administrator on 14-8-5.
 */
public class SysLogTypeMissing implements SysLogTypeManager.SysLogTypeMissingHandler {

    private Connection connection;
    private static final Logger logger = LoggerFactory.getLogger(SysLogTypeMissing.class);

    public SysLogTypeMissing() {
        String driver = SysLogConfig.getInstance().tryGet(SysLogConfig.TARGET_DRIVER);
        String url = SysLogConfig.getInstance().tryGet(SysLogConfig.TARGET_URL);
        String user = SysLogConfig.getInstance().tryGet(SysLogConfig.TARGET_USER);
        String password = SysLogConfig.getInstance().tryGet(SysLogConfig.TARGET_PASSWORD);

        try {
            Class.forName(driver);
            DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public SysLogType handle(SysLogType entry) {
        return null;
    }

    private SysLogType findLogType(String featureCode) {
        PreparedStatement statement = null;
        SysLogType logType = null;
        try {
            statement = connection.prepareStatement(
                    "Select Id From syslog_log_type Where FeatureCode=?");
            statement.setString(1, featureCode);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                logType = new SysLogType();
                logType.setLogId(resultSet.getInt("Id"));
                logType.setFeatureCode(featureCode);
            }
        } catch (SQLException ex) {
            if(logger.isErrorEnabled()) {
                logger.error("put update failed", ex);
            }
        }
        finally {
            if(statement != null) {
                try{
                    statement.close();
                } catch (Exception ex) {

                }
            }
        }
        return logType;
    }

    private void addLogType(SysLogType entry) {
        PreparedStatement statement = null;
        SysLogType logType = null;

        try {
            statement = connection.prepareStatement("Insert Into syslog_log_type(TypeCode, LogLevel, " +
                    "LogMessage, CallStack, AppId, RecentTime, FeatureCode) Values (?,?,?,?,?,?,?)");
            statement.setString(1, entry.getLogTypeCode());
            statement.setInt(2, entry.getLogLevel());
            statement.setString(3, entry.getLogMessage());
            statement.setString(4, entry.getCallStack());
            statement.setInt(5, entry.getAppId());
            statement.setObject(6, entry.getRecentDate());
            statement.setString(7, entry.getFeatureCode());
            statement.execute();
        } catch (SQLException ex) {
            if(logger.isErrorEnabled()) {
                logger.error("put update failed", ex);
            }
        }
        finally {
            if(statement != null) {
                try{
                    statement.close();
                } catch (Exception ex) {

                }
            }
        }
    }
}
