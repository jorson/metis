package com.huayu.metis.entry;

import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

/**
 * Created by Administrator on 14-7-11.
 */
public class VisitLogEntry extends BaseLogEntry {

    protected Text referPage;
    protected Text visitPage;
    protected Text visitPageParam;
    protected long visitTime;

    public VisitLogEntry() {
        super();
        referPage = new Text();
        visitPage = new Text();
        visitPageParam = new Text();
        visitTime = System.currentTimeMillis();
    }

    /**
     * 将原始输入字符串分解,并转换为实体
     * @param valueString 原始字符串
     * 原始字符串格式: [UcCode]\t[UserId]\t[AppId]\t[TerminalCode]\t[IpAddress]\t[ReferPage]\t[visitPage]\t[visitPageParam]\t[VisitTime]
     */
    @Override
    public void parse(String valueString) throws ParseException {
        //使用\t进行数据分割
        String[] values = valueString.split("\t");
        if(values.length != 9) {
            return;
        }
        this.ucCode = new Text(values[0]);
        this.userId = Integer.parseInt(values[1]);
        this.appId = Integer.parseInt(values[2]);
        this.terminalCode = Integer.parseInt(values[3]);
        this.ipAddress = Long.parseLong(values[4]);
        this.referPage = new Text(values[5]);
        this.visitPage = new Text(values[6]);
        this.visitPageParam = new Text(values[7]);
        //转换注册日期
        Date regDate = sdf.parse(values[8]);
        this.visitTime = regDate.getTime();
    }

    @Override
    public String toString() {
        return String.format("%s,%d,%d,%s,%d,%d,%d",
                this.ucCode, this.userId, this.appId,
                this.visitPage, this.terminalCode, this.ipAddress, this.visitTime);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        referPage.readFields(in);
        visitPage.readFields(in);
        visitPageParam.readFields(in);
        visitTime = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        referPage.write(out);
        visitPage.write(out);
        visitPageParam.write(out);
        out.writeLong(visitTime);
    }

    public Text getVisitPage() {
        return visitPage;
    }

    public Text getVisitPageParam() {
        return visitPageParam;
    }

    public long getVisitTime() {
        return visitTime;
    }
}
