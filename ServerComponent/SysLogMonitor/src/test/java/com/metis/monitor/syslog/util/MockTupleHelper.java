package com.metis.monitor.syslog.util;

import backtype.storm.tuple.Tuple;
import com.metis.monitor.syslog.entry.OriginalSysLog;
import com.metis.monitor.syslog.entry.SysLogDetail;
import org.junit.Test;

import java.util.Date;
import java.util.Queue;
import java.util.Random;

import static org.mockito.Mockito.*;

/**
 * Created by Administrator on 14-8-7.
 */
public class MockTupleHelper {

    private static final int[] appId = new int[] {7, 19, 20, 35, 17};
    private static final int[] logLevel = new int[] {1, 2, 3, 4, 5};
    private static final String[] logMessage = new String[]
            {
                    "LogMessage1",
                    "LogMessage2",
                    "LogMessage3",
                    "LogMessage4",
                    "LogMessage5"
            };
    private static final String[] logCallStack = new String[]
            {
                    "LogCallStack1",
                    "LogCallStack2",
                    "LogCallStack3",
                    "LogCallStack4",
                    "LogCallStack5"
            };
    private static Random random = new Random();

    @Test
    public static Tuple mockOriginalSysLogTuple() {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getValueByField(ConstVariables.SYS_LOG_ORIGINAL_VALUE_FILED))
                .thenReturn(getRandomOriginalSysLog());
        return tuple;
    }

    public static Tuple mockSysLogDetailTuple() {
        Tuple tuple = mock(Tuple.class);
        Object detailObj = getRandomSysLogDetail();
        Integer appId = ((SysLogDetail)detailObj).getAppId();
        when(tuple.getValueByField(ConstVariables.SYS_LOG_DETAIL_VALUE_FILED))
                .thenReturn(detailObj);
        when(tuple.getIntegerByField(ConstVariables.SYS_LOG_ORIGINAL_PARTITION_FIELD))
                .thenReturn(appId);
        return tuple;
    }

    private static Object getRandomSysLogDetail() {
        int rndNum = random.nextInt(5);

        SysLogDetail logDetail = new SysLogDetail(logLevel[rndNum], appId[rndNum],
                new Date(System.currentTimeMillis()));
        return logDetail;
    }

    private static Object getRandomOriginalSysLog() {
        int rndNum = random.nextInt(5);

        OriginalSysLog sysLog = new OriginalSysLog();
        sysLog.setLogLevel(logLevel[rndNum]);
        sysLog.setAppId(appId[rndNum]);
        sysLog.setCallStack(logCallStack[rndNum]);
        sysLog.setLogMessage(logMessage[rndNum]);
        sysLog.setLogDate(new Date(System.currentTimeMillis()));
        sysLog.generateFeatureCode();

        return sysLog;
    }
}
