package com.huayu.metis.flume.utility;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ND on 14-4-18.
 */
public class CsvUtils {
    /**
     * 默认的栏位分隔符
     */
    public static final char DEFAULT_FIELD_SEPARATOR_CHAR = ',';

    /**
     * 默认的字符集
     */
    public static final String DEFAULT_CHARSET = "UTF-8";

    public static String join(String[] datas) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            CsvWriter wr = new CsvWriter(baos, DEFAULT_FIELD_SEPARATOR_CHAR, Charset.forName(DEFAULT_CHARSET));
            wr.writeRecord(datas);
            wr.close();
            return baos.toString();
        } catch (IOException e) {
            return null;//ArrayUtils.join(datas, String.valueOf(DEFAULT_FIELD_SEPARATOR_CHAR));
        }
    }


    public static String[] split(String record) {
        CsvReader reader = null;
        try {
            ByteArrayInputStream stream = new ByteArrayInputStream(record.getBytes());
            reader = new CsvReader(stream, DEFAULT_FIELD_SEPARATOR_CHAR, Charset.forName(DEFAULT_CHARSET));    //一般用这编码读就可以了
            while (reader.readRecord()) {
                return reader.getValues();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        return null;
    }

    public static List<String[]> splitMultiLines(String record) {
        CsvReader reader = null;
        List<String[]> csvList = new ArrayList<String[]>(); //用来保存数据
        try {
            ByteArrayInputStream stream = new ByteArrayInputStream(record.getBytes());
            reader = new CsvReader(stream, DEFAULT_FIELD_SEPARATOR_CHAR, Charset.forName(DEFAULT_CHARSET));    //一般用这编码读就可以了
            //reader.readHeaders(); // 跳过表头   如果需要表头的话，不要写这句。
            while (reader.readRecord()) { //逐行读入除表头的数据
                csvList.add(reader.getValues());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        return csvList;
    }
}
