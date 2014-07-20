/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huayu.metis.flume.sink.hdfs;

import java.io.IOException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSSequenceFile extends AbstractHDFSWriter {

  private static final Logger logger = LoggerFactory.getLogger(HDFSSequenceFile.class);
  private SequenceFile.Writer writer;
  private String writeFormat;
  private Context serializerContext;
  private SequenceFileSerializer serializer;
  private FSDataOutputStream outStream = null;

  public HDFSSequenceFile() {
    writer = null;
  }

  @Override
  public void configure(Context context) {
    super.configure(context);
    //默认使用文本写入方式
    writeFormat = context.getString("hdfs.writeFormat", SequenceFileSerializerType.Text.name());
    serializerContext = new Context(context.getSubProperties(SequenceFileSerializerFactory.CTX_PREFIX));
    serializer = SequenceFileSerializerFactory.getSerializer(writeFormat, serializerContext);
    logger.info("writeFormat = " + writeFormat);
  }

  @Override
  public void open(String filePath) throws IOException {
      Configuration conf = new Configuration();
      Path dstPath = new Path(filePath);

      FileSystem fileSystem = dstPath.getFileSystem(conf);
      //因为使用2.2版本的Hadoop, dfs.append.support 这个配置已经不存在
      if(fileSystem.exists(dstPath) && fileSystem.isFile(dstPath)) {
          outStream = fileSystem.append(dstPath);
      } else{
          outStream = fileSystem.create(dstPath);
      }

      writer = SequenceFile.createWriter(conf,
              SequenceFile.Writer.stream(outStream),
              SequenceFile.Writer.keyClass(serializer.getKeyClass()),
              SequenceFile.Writer.valueClass(serializer.getValueClass()));
      registerCurrentStream(outStream, fileSystem, dstPath);
  }

  @Override
  public void append(Event e) throws IOException {
    for (SequenceFileSerializer.Record record : serializer.serialize(e)) {
      writer.append(record.getKey(), record.getValue());
    }
  }

  @Override
  public void sync() throws IOException {
    writer.hsync();
  }

  @Override
  public void close() throws IOException {
    writer.close();
    closeHDFSOutputStream(outStream);
    unregisterCurrentStream();
  }
}
