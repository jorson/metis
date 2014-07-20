package com.huayu.metis.io;

import com.huayu.metis.entry.RegisterLogEntry;
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
        conf.set("fs.default.name", "hdfs://Jorson-Linux:10000");

        URI hdfsPath = new URI("hdfs://Jorson-Linux:10000/orignal/1/api/14-07-16");
        Path path = new Path(hdfsPath);

        //先判断文件夹是否存在, 如果不存在, 则先创建文件夹
        FileSystem fs = FileSystem.get(conf);
        if(!fs.exists(path)){
            fs.mkdirs(path);
        }

        String[] files = new String[] {
                "hdfs://Jorson-Linux:10000/orignal/1/api/14-07-16/part1.log",
                "hdfs://Jorson-Linux:10000/orignal/1/api/14-07-16/part2.log",
                "hdfs://Jorson-Linux:10000/orignal/1/api/14-07-16/part3.log",
                "hdfs://Jorson-Linux:10000/orignal/1/api/14-07-16/part4.log",
                "hdfs://Jorson-Linux:10000/orignal/1/api/14-07-16/part5.log",
        };
        long counter = 0;
        SequenceFile.Writer writer;
        for(int j=0; j<5; j++) {
            Path partFile = new Path(files[j]);
            writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(partFile),
                    SequenceFile.Writer.keyClass(LongWritable.class),
                    SequenceFile.Writer.valueClass(Text.class));

            for(int i=0; i<100; i++) {
                counter++;
                Text value = new Text("auc\t10001\t19\t1\t1001\t3264456\t2014-07-17 15:15:15");
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
        conf.set("fs.default.name", "hdfs://Jorson-Linux:10000");

        URI srcPath = new URI("hdfs://Jorson-Linux:10000/orignal/1/api/14-07-16");
        URI targetFileUri = new URI("hdfs://Jorson-Linux:10000/orignal/1/api/combine.seq");

        //先删除掉已经存在的文件
        FileSystem fs = FileSystem.get(conf);
        Path targetPath = new Path(targetFileUri);
        if(fs.exists(targetPath)) {
            fs.delete(targetPath, false);
        }

        SequenceFileCombiner.combineFile(conf, srcPath, targetFileUri, RegisterLogEntry.class);
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
}
