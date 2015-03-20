package com.metis.monitor.syslog.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import com.metis.monitor.syslog.config.SysLogConfig;
import com.metis.monitor.syslog.util.MockTupleHelper;
import com.metis.monitor.syslog.util.SysLogTypeManager;
import com.metis.monitor.syslog.util.SysLogTypeMissing;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.ParseException;
import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * Created by Administrator on 14-8-7.
 */
public class BoltTest {

    private static backtype.storm.Config config;

    @BeforeClass
    public static void setup() throws Exception {
        String configPath = "D:\\Documents\\metis_github\\ServerComponent\\SysLogMonitor\\src\\test\\resources\\monitor.syslog.properties";
        config = SysLogConfig.getInstance().loadConfig(configPath);
    }

    @Test
    public void OriginalBoltTest() {
        Tuple originalInput = MockTupleHelper.mockOriginalInput();
        OriginalParseBolt bolt = new OriginalParseBolt();

        Map conf = mock(Map.class);
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector collector = mock(OutputCollector.class);
        bolt.prepare(conf, context, collector);

        //execute
        bolt.execute(originalInput);

        //verify
        verify(collector).ack(originalInput);
    }

    @Test
    public void TransportBoltTest() throws ParseException {
        Tuple originalSysLog = MockTupleHelper.mockOriginalSysLogTuple();
        TransportBolt bolt = new TransportBolt();
        Map conf = mock(Map.class);
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector collector = mock(OutputCollector.class);
        bolt.prepare(config, context, collector);

        //execute
        bolt.execute(originalSysLog);

        //verify
        verify(collector).ack(originalSysLog);
    }

    @Test
    public void BatchingBolt() throws InterruptedException {
        BatchingBolt bolt = new BatchingBolt();
        Map conf = mock(Map.class);
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector collector = mock(OutputCollector.class);

        bolt.prepare(conf, context, collector);
        for(int i=0; i<120; i++) {
            Tuple detailSysLog = MockTupleHelper.mockSysLogDetailTuple();
            bolt.execute(detailSysLog);
            Thread.sleep(1000);
        }
    }
}
