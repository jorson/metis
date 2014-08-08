package com.metis.monitor.syslog.entry;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

/**
 * 系统日志的详细信息实体
 * Created by Administrator on 14-8-6.
 */
public class SysLogDetail implements Serializable {

    private static final long serialVersionUID = -1L;

    private Integer logTypeId;
    private Integer appId;
    private Date logDate;

    public SysLogDetail(Integer logTypeId, Integer appId, Date logDate) {
        this.logTypeId = logTypeId;
        this.appId = appId;
        this.logDate = logDate;
    }

    public Integer getLogTypeId() {
        return logTypeId;
    }

    public Integer getAppId() {
        return appId;
    }

    public Date getLogDate() {
        return logDate;
    }
}
