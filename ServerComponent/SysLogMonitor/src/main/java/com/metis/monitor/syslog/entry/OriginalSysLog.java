package com.metis.monitor.syslog.entry;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.Serializable;
import java.util.Date;

/**
 * 原始日志的实体类
 * Created by Administrator on 14-8-6.
 */
public class OriginalSysLog implements Serializable {

    private static final long serialVersionUID = -1L;

    private Integer appId;
    private Integer logLevel;
    private String logMessage;
    private String callStack;
    private Date logDate;
    private String featureCode;

    public OriginalSysLog() {

    }

    public void generateFeatureCode() {
        this.featureCode = DigestUtils.md5Hex(
                String.format("%d-%s-%s", this.logLevel, this.logMessage, this.callStack));
    }

    public Integer getAppId() {
        return appId;
    }

    public void setAppId(Integer appId) {
        this.appId = appId;
    }

    public Integer getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(Integer logLevel) {
        this.logLevel = logLevel;
    }

    public String getLogMessage() {
        return logMessage;
    }

    public void setLogMessage(String logMessage) {
        this.logMessage = logMessage;
    }

    public String getCallStack() {
        return callStack;
    }

    public void setCallStack(String callStack) {
        this.callStack = callStack;
    }

    public Date getLogDate() {
        return logDate;
    }

    public void setLogDate(Date logDate) {
        this.logDate = logDate;
    }

    public String getFeatureCode() {
        return featureCode;
    }

    public void setFeatureCode(String featureCode) {
        this.featureCode = featureCode;
    }
}
