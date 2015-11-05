package com.lab.epam.entity;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Bohdan-Dmytro_Vovchu on 11/4/2015.
 */
public class Model implements Writable {
    private DoubleWritable averageBytesPerRequest;
    private DoubleWritable totalBytes;

    public Model() {
        averageBytesPerRequest = new DoubleWritable();
        totalBytes = new DoubleWritable();
    }

    public Model(DoubleWritable totalBytes, DoubleWritable averageBytesPerRequest) {
        this.totalBytes = totalBytes;
        this.averageBytesPerRequest = averageBytesPerRequest;
    }

    @Override
    public String toString() {
        return "" +averageBytesPerRequest +
                ", " + totalBytes;
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        averageBytesPerRequest.write(dataOutput);
        totalBytes.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        averageBytesPerRequest.readFields(dataInput);
        totalBytes.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Model model = (Model) o;

        if (averageBytesPerRequest != null ? !averageBytesPerRequest.equals(model.averageBytesPerRequest) : model.averageBytesPerRequest != null)
            return false;
        return !(totalBytes != null ? !totalBytes.equals(model.totalBytes) : model.totalBytes != null);

    }

    @Override
    public int hashCode() {
        int result = averageBytesPerRequest != null ? averageBytesPerRequest.hashCode() : 0;
        result = 31 * result + (totalBytes != null ? totalBytes.hashCode() : 0);
        return result;
    }
}
