package com.lab.epam.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Bohdan-Dmytro_Vovchu on 11/6/2015.
 */
public class ResultModel implements Writable {

    private long avg;
    private long totalBytes;

    @Override
    public String toString() {
        return "" + avg +
                ", " + totalBytes;
    }

    public long getAvg() {
        return avg;
    }

    public void setAvg(long avg) {
        this.avg = avg;
    }

    public ResultModel() {
        avg = 0;
        totalBytes = 0;
    }

    public ResultModel(long avg, long totalBytes) {
        this.avg = avg;
        this.totalBytes = totalBytes;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(avg);
        dataOutput.writeLong(totalBytes);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        avg = dataInput.readLong();
        totalBytes = dataInput.readInt();
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public void setTotalBytes(long totalBytes) {
        this.totalBytes = totalBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ResultModel that = (ResultModel) o;

        if (avg != that.avg) return false;
        return totalBytes == that.totalBytes;

    }

    @Override
    public int hashCode() {
        int result = (int) (avg ^ (avg >>> 32));
        result = 31 * result + (int) (totalBytes ^ (totalBytes >>> 32));
        return result;
    }
}
