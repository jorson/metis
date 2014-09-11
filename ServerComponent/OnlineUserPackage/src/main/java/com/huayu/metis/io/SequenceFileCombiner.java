package com.huayu.metis.io;

import com.huayu.metis.entry.BaseLogEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * 指定特定文件夹,将其中的小文件合并为一个大文件
 * Created by Administrator on 14-7-16.
 */
public class SequenceFileCombiner {

    /**
     * 将指定目录下文件合并为一个文件
     * @param srcFolder 原始文件所在文件夹
     * @param targetFileUri 目标文件存放路径
     */
    public static <TEntry extends BaseLogEntry> void combineFile(
            Configuration conf, URI srcFolder, URI targetFileUri,
            Class<TEntry> valueCls) throws IOException {
        //先判断文件是否存在
        FileSystem fs = FileSystem.get(conf);
        Path targetPath = new Path(targetFileUri);
        //如果文件已经存在,则直接退出
        if(fs.exists(targetPath)){
            return;
        }
        //如果文件不存在,开始创建目录
        Path srcPath = new Path(srcFolder);
        //如果源文件夹不存在
        if(!fs.exists(srcPath)) {
            return;
        }
        //获取原始文件夹下的文件列表
        List<String> orgFileList = getSubFiles(new Path(srcFolder), conf);
        //声明计数器
        long counter = 0;

        //创建合并日志的Writer
        SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(targetPath),
                SequenceFile.Writer.keyClass(LongWritable.class),
                SequenceFile.Writer.valueClass(valueCls));
        //声明原始日志的读取者
        SequenceFile.Reader reader;
        LongWritable writeKey = new LongWritable(0L);

        //循环读取数据
        for(int i=0; i<orgFileList.size(); i++) {
            //创建Reader
            reader = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file(new Path(orgFileList.get(i))));
            LongWritable key = new LongWritable(0);
            Text value = new Text();

            while (reader.next(key, value)) {
                writeKey.set(counter);
                ++counter;

                try{
                    TEntry entry = valueCls.newInstance();
                    entry.parse(value.toString());
                    writer.append(writeKey, entry);
                } catch(Exception e){
                    continue;
                }
            }
            //关闭Reader
            IOUtils.closeStream(reader);
        }
        //完成全部写入
        IOUtils.closeStream(writer);
    }

    /**
     * 将指定的一组文件合并为一个文件
     * 被合并的文件中的Value类型已经不是Text,而是参数中ValueCLS
     */
    public static <TEntry extends BaseLogEntry> void combineFile(
            Configuration conf, List<URI> srcFiles, URI targetFile,
            Class<TEntry> valueCls) throws IOException, IllegalAccessException, InstantiationException {
        //先判断文件是否存在
        FileSystem fs = FileSystem.get(conf);
        Path targetPath = new Path(targetFile);
        //如果文件已经存在,则直接退出
        if(fs.exists(targetPath)){
            return;
        }
        //声明计数器
        long counter = 0;
        //创建合并日志的Writer
        SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(targetPath),
                SequenceFile.Writer.keyClass(LongWritable.class),
                SequenceFile.Writer.valueClass(valueCls));
        //声明原始日志的读取者
        SequenceFile.Reader reader;
        LongWritable writeKey = new LongWritable(0L);

        //循环所有的原始文件路径
        for(int i=0; i<srcFiles.size(); i++) {
            Path path = new Path(srcFiles.get(i));
            if(!fs.exists(path)) {
                continue;
            }

            //创建Reader
            reader = new SequenceFile.Reader(conf,
                    SequenceFile.Reader.file(path));
            LongWritable key = new LongWritable(0);
            TEntry value = valueCls.newInstance();

            while(reader.next(key, value)) {
                writeKey.set(counter);
                ++counter;

                try{
                    writer.append(writeKey, value);
                } catch(Exception e){
                    continue;
                }
            }
            //关闭Reader
            IOUtils.closeStream(reader);
        }
        //完成全部写入
        IOUtils.closeStream(writer);
    }

    public static List<String> getSubFiles(Path srcFolder, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);
        List<String> fileList = new ArrayList<String>();
        try {
            FileStatus[] fileStatuses = fileSystem.listStatus(srcFolder);
            for(FileStatus file : fileStatuses) {
                fileList.add(file.getPath().toUri().getPath());
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        return fileList;
    }
}
