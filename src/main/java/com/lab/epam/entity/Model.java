package com.lab.epam.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Bohdan-Dmytro_Vovchu on 11/4/2015.
 */
public class Model implements Writable {

    @Override
    public String toString() {
        return "" + totalBytes +
                ", " + count;
    }

    private long totalBytes;
    private int count;

    public long getTotalBytes() {
        return totalBytes;
    }

    public void setTotalBytes(long totalBytes) {
        this.totalBytes = totalBytes;
    }

    public Model() {
        totalBytes =0;
        count = 0;
    }

    public Model(long totalBytes, int count) {
        this.totalBytes = totalBytes;
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(totalBytes);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        totalBytes = dataInput.readLong();
        count = dataInput.readInt();
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Model model = (Model) o;

        if (totalBytes != model.totalBytes) return false;
        return count == model.count;

    }

    @Override
    public int hashCode() {
        int result = (int) (totalBytes ^ (totalBytes >>> 32));
        result = 31 * result + count;
        return result;
    }
}
