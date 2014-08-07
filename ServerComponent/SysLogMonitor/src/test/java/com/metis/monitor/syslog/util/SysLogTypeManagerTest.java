package com.metis.monitor.syslog.util;

import com.metis.monitor.syslog.config.SysLogConfig;
import com.metis.monitor.syslog.entry.SysLogType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Date;
import java.util.UUID;

/**
 * Created by Administrator on 14-8-6.
 */
public class SysLogTypeManagerTest {

    private static SysLogTypeManager manager;

    @BeforeClass
    public static void setup() throws Exception {
        String configPath = "E:\\CodeInGit\\metis_github\\ServerComponent\\SysLogMonitor\\src\\test\\resources\\monitor.syslog.properties";
        SysLogConfig.getInstance().loadConfig(configPath);

        SysLogTypeMissing missingHandler = new SysLogTypeMissing();
        manager = SysLogTypeManager.getInstance(missingHandler);
    }

    @Test
    public void getNotExistTest() {
        SysLogType entry = new SysLogType();
        entry.setLogTypeCode(UUID.randomUUID().toString().toUpperCase());
        entry.setAppId(1);
        entry.setLogLevel(1);
        entry.setCallStack("Call Stack");
        entry.setLogMessage("Log Message");
        entry.setRecentDate(new Date());
        entry.generateFeatureCode();

        Integer result = manager.get(entry);
        System.out.println(result);
    }

    @Test
    public void getExistsTest() {
        SysLogType entry = new SysLogType();
        entry.setLogTypeCode(UUID.randomUUID().toString().toUpperCase());
        entry.setAppId(1);
        entry.setLogLevel(1);
        entry.setCallStack("Call Stack");
        entry.setLogMessage("Log Message");
        entry.setRecentDate(new Date());
        entry.generateFeatureCode();

        Integer result = manager.get(entry);
        System.out.println(result);
    }
}
