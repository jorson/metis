package com.huayu.metis.io;

import com.huayu.metis.entry.RegisterLogEntry;
import com.huayu.metis.entry.VisitLogEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Random;

/**
 * Created by Administrator on 14-7-16.
 */
public class SequenceFileCombinerTest {

    @Before
    public void setup(){

    }

    @Test
    public void createSplitFile() throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192168-072166:10000");

        URI hdfsPath = new URI("hdfs://192168-072166:10000/original/20140826/visit");
        Path path = new Path(hdfsPath);

        //先判断文件夹是否存在, 如果不存在, 则先创建文件夹
        FileSystem fs = FileSystem.get(conf);
        if(!fs.exists(path)){
            fs.mkdirs(path);
        }

        String[] files = new String[] {
                "hdfs://192168-072166:10000/original/20140826/visit/part1.log",
                "hdfs://192168-072166:10000/original/20140826/visit/part2.log",
                "hdfs://192168-072166:10000/original/20140826/visit/part3.log",
                "hdfs://192168-072166:10000/original/20140826/visit/part4.log",
                "hdfs://192168-072166:10000/original/20140826/visit/part5.log",
        };
        long counter = 0;
        SequenceFile.Writer writer;
        Integer[] appIds = new Integer[]{1,2,3,4,5,6};
        Long[] userIds = new Long[] {100011L,100010L,10090L,10008L,10007L,10006L,10005L,10004L,10003L,10002L};
        Random rnd = new Random();

        for(int j=0; j<5; j++) {
            Path partFile = new Path(files[j]);
            writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(partFile),
                    SequenceFile.Writer.keyClass(LongWritable.class),
                    SequenceFile.Writer.valueClass(Text.class));

            for(int i=0; i<5000; i++) {
                counter++;
                int order = rnd.nextInt(5);
                int uOrder = rnd.nextInt(9);

                String orgText = String.format("auc\t%d\t%d\t1001\t3264456\trefer.page\tvisit.page\tvisit.param\t2014-08-26 15:15:15",
                        userIds[uOrder], appIds[order].intValue());
                Text value = new Text(orgText);
                writer.append(new LongWritable(counter), value);
            }
            IOUtils.closeStream(writer);
        }
    }

    @Test
    public void getSubFilesTest() throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://Jorson-Linux:10000");

        URI hdfsPath = new URI("hdfs://Jorson-Linux:10000/orignal/1/api/14-07-16");
        Path path = new Path(hdfsPath);
        List<String> fileList = SequenceFileCombiner.getSubFiles(path, conf);

        Assert.assertFalse(fileList.isEmpty());
        for(String file : fileList) {
            System.out.println(file);
        }
    }

    @Test
    public void combineFileTest() throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192168-072166:10000");

        URI srcPath = new URI("hdfs://192168-072166:10000/original/20140915/visit/part.1410778420112.log");
        URI targetFileUri = new URI("hdfs://192168-072166:10000/combine/daily/20140915/combine_test.seq");

        //先删除掉已经存在的文件
        FileSystem fs = FileSystem.get(conf);
        Path targetPath = new Path(targetFileUri);
        if(fs.exists(targetPath)) {
            fs.delete(targetPath, false);
        }

        SequenceFileCombiner.combineFile(conf, srcPath, targetFileUri, VisitLogEntry.class);
    }

    @Test
    public void readCombineFileTest() throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://Jorson-Linux:10000");

        URI targetFileUri = new URI("hdfs://Jorson-Linux:10000/orignal/1/api/combine.seq");
        //先删除掉已经存在的文件
        FileSystem fs = FileSystem.get(conf);
        Path targetPath = new Path(targetFileUri);
        if(!fs.exists(targetPath)) {
            System.out.println("file not exists!");
            return;
        }

        SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(targetPath));

        LongWritable key = new LongWritable(0L);
        RegisterLogEntry value = new RegisterLogEntry();

        do{
            System.out.println(String.valueOf(key.get()) + ";" + value.toString());
        }while(reader.next(key, value));
    }

    @Test
    public void combineWeekFileTest() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192168-072166:10000");

        URI[] srcFiles = new URI[]{
                new URI("hdfs://192168-072166:10000/combine/20140725/visit-log.seq"),
                new URI("hdfs://192168-072166:10000/combine/20140726/visit-log.seq"),
                new URI("hdfs://192168-072166:10000/combine/20140727/visit-log.seq"),
                new URI("hdfs://192168-072166:10000/combine/20140728/visit-log.seq"),
        };
        URI targetFile = new URI("hdfs://192168-072166:10000/combine/week/201415/visit-log.seq");
        //先删除掉已经存在的文件
        FileSystem fs = FileSystem.get(conf);
        Path targetPath = new Path(targetFile);
        if(fs.exists(targetPath)) {
            fs.delete(targetPath, false);
        }
        //SequenceFileCombiner.combineFile(conf, new List<URI>(srcFiles, targetFile, VisitLogEntry.class);
    }


    @Test
    public void readMonthCombineFileTest() throws URISyntaxException, IOException {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192168-072166:10000");

        //URI targetFileUri = new URI("hdfs://192168-072166:10000/combine/month/20147/visit-log.seq");
        URI targetFileUri = new URI("hdfs://192168-072166:10000/combine/daily/20140915/visit-log.seq");
        //URI targetFileUri = new URI("hdfs://192168-072166:10000/combine/daily/20140915/combine_test.seq");


        FileSystem fs = FileSystem.get(conf);
        Path targetPath = new Path(targetFileUri);
        if(!fs.exists(targetPath)) {
            System.out.println("file not exists!");
            return;
        }

        SequenceFile.Reader reader = new SequenceFile.Reader(conf,
                SequenceFile.Reader.file(targetPath));

        LongWritable key = new LongWritable(0L);
        VisitLogEntry value = new VisitLogEntry();

        while(reader.next(key, value)) {
            System.out.println(String.valueOf(key.get()) + ";" + value.toString());
        }
        System.out.println(String.valueOf("end"));
    }
}