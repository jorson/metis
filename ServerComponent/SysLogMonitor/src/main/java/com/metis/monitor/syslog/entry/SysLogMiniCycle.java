package com.metis.monitor.syslog.entry;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by Administrator on 14-8-6.
 */
public class SysLogMiniCycle implements Serializable {

    private static final long serialVersionUID = -1L;

    private Date statDate;
    private Integer appId = 0;
    private Integer typeId = 0;
    private Integer logAmount = 0;

    public SysLogMiniCycle() {
    }

    public SysLogMiniCycle(Date logDate, Integer appId, Integer typeId) {
        this.appId = appId;
        this.typeId = typeId;
        this.logAmount = 1;

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(logDate);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);

        this.statDate = calendar.getTime();
    }

    public void addLogAmount() {
        this.logAmount = this.logAmount + 1;
    }

    @Override
    public int hashCode() {
        return typeId + appId;
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof SysLogMiniCycle)) {
            return false;
        }
        SysLogMiniCycle that = (SysLogMiniCycle)obj;
        return this.hashCode() == that.hashCode();
    }

    public Date getStatDate() {
        return statDate;
    }
    public Integer getAppId() {
        return appId;
    }
    public Integer getTypeId() {
        return typeId;
    }
    public Integer getLogAmount() {
        return logAmount;
    }
}
