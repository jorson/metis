package com.huayu.metis.entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 14-7-12.
 */
public class FileOffset implements WritableComparable<FileOffset> {

    private long offset;
    private long lineNumber;
    private String fileName;

    public void readFields(DataInput in) throws IOException {
        this.offset = in.readLong();
        this.fileName = Text.readString(in);
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(offset);
        Text.writeString(out, fileName);
    }

    public int compareTo(FileOffset that) {
        int f = this.fileName.compareTo(that.fileName);
        if(f == 0){
            return (int)Math.signum((double)(this.offset - that.offset));
        }
        return f;
    }

    @Override
    public int hashCode() {
        return 40;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof FileOffset)
            return this.compareTo((FileOffset)obj) == 0;
        return false;
    }

    public void setFilename(String fileName){
        this.fileName = fileName;
    }

    public String getFilename(){
        return this.fileName;
    }

    public void setOffset(long offset){
        this.offset = offset;
    }

    public long getOffset(){
        return this.offset;
    }

    public long getLineNumber() {
        return this.lineNumber;
    }

    public void  setLineNumber(long lineNumber) {
        this.lineNumber = lineNumber;
    }
}
