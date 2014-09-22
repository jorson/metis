package com.huayu.metis.entry;

import com.huayu.metis.mr.example.WordCount;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * Created by Administrator on 14-7-11.
 */
public abstract class BaseLogEntry implements Writable {

    protected Text ucCode;
    protected int userId = 0;
    protected int appId = 0;
    protected long ipAddress = 0;
    protected int terminalCode = 1001;

    protected final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    public BaseLogEntry() {
        ucCode = new Text("auc");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        ucCode.write(out);
        out.writeInt(userId);
        out.writeInt(appId);
        out.writeLong(ipAddress);
        out.writeInt(terminalCode);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ucCode.readFields(in);
        userId = in.readInt();
        appId = in.readInt();
        ipAddress = in.readLong();
        terminalCode = in.readInt();
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        final int prim = 30;
        int result = 1;

        return result + prim + this.ucCode.hashCode()
                + this.userId + this.appId + (int)(this.ipAddress % Integer.MAX_VALUE) + this.terminalCode;
    }

    public abstract void parse(String valueString) throws ParseException;

    public Text getUcCode() {
        return ucCode;
    }

    public int getUserId() {
        return userId;
    }

    public int getAppId() {
        return appId;
    }

    public long getIpAddress() {
        return ipAddress;
    }

    public int getTerminalCode() {
        return terminalCode;
    }
}
