package com.huayu.metis.entry;

import com.huayu.metis.keyvalue.RegisterUserKey;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 原始注册日志实体
 * @author Jorson
 * Created by Administrator on 14-7-11.
 */
public class RegisterLogEntry extends BaseLogEntry {

    protected int registerMode;
    protected long registerTime;

    public RegisterLogEntry() {
        super();
    }

    /**
     * 将原始输入字符串分解,并转换为实体
     * @param valueString 原始字符窗
     * 原始字符串格式: [UcCode]\t[UserId]\t[AppId]\t[RegisterMode]\t[TerminalCode]\t[IpAddress]\t[RegisterTime]
     */
    @Override
    public void parse(String valueString) throws ParseException {
        //使用\t进行数据分割
        String[] values = valueString.split("\t");
        if(values.length != 7) {
            return;
        }
        this.ucCode = new Text(values[0]);
        this.userId = Integer.parseInt(values[1]);
        this.appId = Integer.parseInt(values[2]);
        this.registerMode = Integer.parseInt(values[3]);
        this.terminalCode = Integer.parseInt(values[4]);
        this.ipAddress = Integer.parseInt(values[5]);
        //转换注册日期
        Date regDate = sdf.parse(values[6]);
        this.registerTime = regDate.getTime();
    }

    @Override
    public String toString() {
        return String.format("%s,%d,%d,%d,%d,%d,%d",
                this.ucCode, this.userId, this.appId,
                this.registerMode, this.terminalCode, this.ipAddress, this.registerTime);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        registerMode = in.readInt();
        registerTime = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(registerMode);
        out.writeLong(registerTime);
    }

    public int getRegisterMode() {
        return registerMode;
    }

    public long getRegisterTime() {
        return registerTime;
    }
}
