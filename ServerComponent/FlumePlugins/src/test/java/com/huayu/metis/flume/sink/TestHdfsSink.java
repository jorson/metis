package com.huayu.metis.flume.sink;

import com.google.common.collect.Lists;
import com.huayu.metis.flume.sink.hdfs.HDFSEventSink;
import junit.framework.Assert;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by Administrator on 14-4-18.
 */
public class TestHdfsSink {

    private HDFSEventSink sink;
    private String testPath;
    private static final Logger LOG = LoggerFactory.getLogger(TestHdfsSink.class);

    static {
        System.setProperty("java.security.krb5.realm", "flume");
        System.setProperty("java.security.krb5.kdc", "blah");
    }

    private void dirCleanup(){
        Configuration conf = new Configuration();
        try{
            FileSystem fs = FileSystem.get(conf);
            Path dirPath = new Path(testPath);
            if(fs.exists(dirPath)) {
                fs.delete(dirPath, true);
            }
        } catch(IOException ex) {
            LOG.warn("IO Error in test cleanup", ex);
        }
    }

    private void verifyOutputTextFiles(FileSystem fs, Configuration conf, String dir, String prefix, List<String> bodies) throws IOException {
        int found = 0;
        int expected = bodies.size();
        for(String outputFile : getAllFiles(dir)) {
            String name = (new File(outputFile)).getName();
            if(name.startsWith(prefix)) {
                FSDataInputStream input = fs.open(new Path(outputFile));
                BufferedReader reader = new BufferedReader(new InputStreamReader(input));
                String body = null;
                while((body = reader.readLine()) != null) {
                    bodies.remove(body);
                    found++;
                }
                reader.close();
            }
        }
        Assert.assertTrue("Found = " + found + ", Expected = "  +
                expected + ", Left = " + bodies.size() + " " + bodies,
                bodies.size() == 0);

    }

    private List<String> getAllFiles(String input) {
        List<String> output = Lists.newArrayList();
        File dir = new File(input);
        if(dir.isFile()) {
            output.add(dir.getAbsolutePath());
        } else if(dir.isDirectory()) {
            for(String file : dir.list()) {
                File subDir = new File(dir, file);
                output.addAll(getAllFiles(subDir.getAbsolutePath()));
            }
        }
        return output;
    }

    @Before
    public void setUp(){
        LOG.debug("Starting...");

        testPath = "file:///Jorson-Linux:10000/tmp/flume-test";// + Calendar.getInstance().getTimeInMillis() + "." + Thread.currentThread().getId();
        sink = new HDFSEventSink();
        sink.setName("HdfsSink-" + UUID.randomUUID().toString());
        dirCleanup();
    }

    @After
    public void tearDown(){
        if(System.getenv("hdfs_keepFiles") == null){
            dirCleanup();
        }
    }

    //@Test
    public void testTextBatchAppend() throws Exception {
        doTestTextBatchAppend();
    }

    //@Test
    public void testLifecycle() throws InterruptedException, LifecycleException  {
        LOG.debug("Starting...");
        Context context = new Context();
        context.put("hdfs.path", testPath);
        context.put("hdfs.filePrefix", "pageview");

        Configurables.configure(sink, context);
        sink.setChannel(new MemoryChannel());
        sink.start();
        sink.stop();
    }

    public void doTestTextBatchAppend() throws Exception{
        LOG.debug("Starting...");

        final long rollCount = 10;
        final long batchSize = 2;
        final String fileName = "PageView";

        String newPath = testPath + "/singleTextBucket";
        int totalEvents = 0;
        int i = 1, j = 1;

        //clear the test directory
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path dirPath = new Path(newPath);
        fs.delete(dirPath, true);
        fs.mkdirs(dirPath);

        Context context = new Context();
        context.put("hdfs.path", newPath);
        context.put("hdfs.rollCount", String.valueOf(rollCount));
        context.put("hdfs.batchSize", String.valueOf(batchSize));
        context.put("hdfs.filePrefix", "pageview");

        Channel channel = new MemoryChannel();
        Configurables.configure(channel, context);
        sink.setChannel(channel);
        sink.start();

        Calendar eventDate = Calendar.getInstance();
        Date currentDate = new Date();
        Map<String, String> header = new HashMap<String, String>();
        header.put("topic", "PageView");

        List<String> bodies = Lists.newArrayList();

        //将测试的事件推入到通道中
        for(i=1; i<=(rollCount*10)/batchSize;i++){
            Transaction txn = channel.getTransaction();
            txn.begin();
            for(j=1;j<=batchSize;j++){
                header.put("timestamp", String.valueOf(currentDate.getTime()));
                Event event = new SimpleEvent();
                eventDate.clear();
                eventDate.set(2014, i, i, i ,0);
                String body = "Test." + i + "." + j;

                event.setHeaders(header);
                event.setBody(body.getBytes());
                bodies.add(body);
                channel.put(event);
                totalEvents++;
            }
            txn.commit();
            txn.close();

            //execute sink to process the events
            sink.process();
        }
        sink.stop();

        FileStatus[] dirStat = fs.listStatus(dirPath);
        Path fList[] = FileUtil.stat2Paths(dirStat);

        long expectedFiles = totalEvents / rollCount;
        if(totalEvents % rollCount > 0){
            expectedFiles++;
        }

        Assert.assertEquals("num files wrong, found: " + Lists.newArrayList(fList), expectedFiles, fList.length);
        //检查所有写入文件的内容
        verifyOutputTextFiles(fs, conf, dirPath.toUri().getPath(), fileName, bodies);
    }
}
