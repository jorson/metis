package com.metis.monitor.syslog.util;

import com.metis.monitor.syslog.entry.SysLogType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.UUID;

/**
 * Created by Administrator on 14-8-5.
 */
public class SysLogTypeMissing implements SysLogTypeManager.SysLogTypeMissingHandler {

/*    private Connection connection;*/
    private static final Logger logger = LoggerFactory.getLogger(SysLogTypeMissing.class);

    public SysLogTypeMissing() {
/*        String driver = SysLogConfig.getInstance().tryGet(SysLogConfig.TARGET_DRIVER);
        String url = SysLogConfig.getInstance().tryGet(SysLogConfig.TARGET_URL);
        String user = SysLogConfig.getInstance().tryGet(SysLogConfig.TARGET_USER);
        String password = SysLogConfig.getInstance().tryGet(SysLogConfig.TARGET_PASSWORD);

        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }*/
    }

    @Override
    public SysLogType handle(SysLogType entry) {
        if("".equals(entry.getFeatureCode()) || entry.getFeatureCode() == null) {
            return entry;
        }

        SysLogType item;
        synchronized (SysLogType.class) {
            item = findLogType(entry.getFeatureCode());
            if(item == null) {
                synchronized (SysLogType.class) {
                    //创建一个新的LogTypeCode
                    entry.setLogTypeCode(UUID.randomUUID().toString().toUpperCase());
                    addLogType(entry);
                    return entry;
                }
            } else {
                return item;
            }
        }
    }

    private SysLogType findLogType(String featureCode) {
        PreparedStatement statement = null;
        SysLogType logType = null;
        Connection connection = null;
        try {
            connection = C3P0Utils.getInstance().getConnection();
            if(connection == null) {
                if(logger.isErrorEnabled()) {
                    logger.error("BATCHING_BOLT", "Get Connection from C3P0Utils IS NULL");
                }
                return logType;
            }
            statement = connection.prepareStatement(
                    "Select Id, FeatureCode From syslog_log_type Where FeatureCode=?");
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
        } finally {
            if(statement != null) {
                try{
                    statement.close();
                } catch (Exception ex) {

                }
            }
            if(connection != null) {
                try {
                    connection.close();
                } catch (Exception ex) {

                }
            }
        }
        return logType;
    }

    private void addLogType(SysLogType entry) {
        PreparedStatement statement = null;
        Connection connection = null;
        try {
            connection = C3P0Utils.getInstance().getConnection();
            if(connection == null) {
                if(logger.isErrorEnabled()) {
                    logger.error("BATCHING_BOLT", "Get Connection from C3P0Utils IS NULL");
                }
                return;
            }

            statement = connection.prepareStatement("Insert Into syslog_log_type(TypeCode, LogLevel, " +
                    "LogMessage, CallStack, AppId, RecentTime, FeatureCode) Values (?,?,?,?,?,?,?) " +
                    "On DUPLICATE Key Update RecentTime=CURRENT_TIMESTAMP()",
                    Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, entry.getLogTypeCode());
            statement.setInt(2, entry.getLogLevel());
            statement.setString(3, entry.getLogMessage());
            statement.setString(4, entry.getCallStack());
            statement.setInt(5, entry.getAppId());
            statement.setObject(6, entry.getRecentDate());
            statement.setString(7, entry.getFeatureCode());
            statement.executeUpdate();
            ResultSet resultSet = statement.getGeneratedKeys();
            if(resultSet.next()) {
                Integer id = resultSet.getInt(1);
                entry.setLogId(id);
            }
        } catch (SQLException ex) {
            if(logger.isErrorEnabled()) {
                logger.error("put update failed", ex);
            }
        } finally {
            if(statement != null) {
                try{
                    statement.close();
                } catch (Exception ex) {

                }
            }
            if(connection != null) {
                try {
                    connection.close();
                } catch (Exception ex) {

                }
            }
        }
    }
}
