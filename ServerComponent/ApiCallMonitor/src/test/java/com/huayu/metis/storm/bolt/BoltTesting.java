package com.huayu.metis.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.huayu.metis.storm.bolt.monitor.ApiMonitorAvgBolt;
import com.huayu.metis.storm.bolt.monitor.ApiMonitorCountingBolt;
import com.huayu.metis.storm.bolt.monitor.ApiMonitorGroupingBolt;
import com.huayu.metis.storm.bolt.monitor.ApiMonitorSumBolt;
import com.huayu.metis.storm.tool.MockTupleHelpers;

import static org.mockito.Mockito.*;

import org.joda.time.DateTime;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

/**
 * Created by Administrator on 14-5-4.
 */
public class BoltTesting {

    @Test
    public void groupingBoltTest() {

        Tuple tickTuple = MockTupleHelpers.mockTuple();
        ApiMonitorGroupingBolt bolt = new ApiMonitorGroupingBolt();
        Map conf = mock(Map.class);
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector collector = mock(OutputCollector.class);
        bolt.prepare(conf, context, collector);

        //when
        bolt.execute(tickTuple);

        //then
        verify(collector).ack(tickTuple);
    }

    @Test
    public void couningBoltTest() {
        Tuple coutingTuple = MockTupleHelpers.mockGroupedTuple();
        ApiMonitorCountingBolt bolt = new ApiMonitorCountingBolt();

        Map conf = mock(Map.class);
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector collector = mock(OutputCollector.class);
        bolt.prepare(conf, context, collector);

        //when
        bolt.execute(coutingTuple);

        //then
        verify(collector).ack(coutingTuple);
    }

    @Test
    public void sumBoltTest() {
        Tuple sumTuple = MockTupleHelpers.mockGroupedTuple();
        ApiMonitorSumBolt bolt = new ApiMonitorSumBolt(new String[]{"requestSize", "responseSize"});

        Map conf = mock(Map.class);
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector collector = mock(OutputCollector.class);
        bolt.prepare(conf, context, collector);

        //when
        bolt.execute(sumTuple);

        //then
        verify(collector).ack(sumTuple);
    }

    @Test
    public void avgBoltTest() {
        Tuple sumTuple = MockTupleHelpers.mockGroupedTuple();
        ApiMonitorAvgBolt bolt = new ApiMonitorAvgBolt(new String[]{"responseTime"});

        Map conf = mock(Map.class);
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector collector = mock(OutputCollector.class);
        bolt.prepare(conf, context, collector);

        //when
        bolt.execute(sumTuple);

        //then
        verify(collector).ack(sumTuple);
    }

    @Test
    public void getMinSecond() {

        DateTime time = new DateTime(2014, 4, 15, 12, 34, 51);
        System.out.println(time.getMillis());
    }
}
