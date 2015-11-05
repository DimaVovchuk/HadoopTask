package com.lab.epam.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Bohdan-Dmytro_Vovchu on 11/4/2015.
 */
public class Model implements Writable {
    private Double averageBytesPerRequest;
    private Double totalBytes;

    @Override
    public String toString() {
        return "    " +
                "averageBytesPerRequest=" + averageBytesPerRequest +
                ", totalBytes=" + totalBytes;
    }

    public Double getTotalBytes() {
        return totalBytes;
    }

    public void setTotalBytes(Double totalBytes) {
        this.totalBytes = totalBytes;
    }

    public Double getBytesPerRequest() {
        return averageBytesPerRequest;
    }

    public void setBytesPerRequest(Double bytesPerRequest) {
        this.averageBytesPerRequest = bytesPerRequest;
    }

    public Model(Double totalBytes, Double averageBytesPerRequest) {
        this.totalBytes = totalBytes;
        this.averageBytesPerRequest = averageBytesPerRequest;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeChars(String.valueOf(averageBytesPerRequest));
        dataOutput.writeChars(String.valueOf(totalBytes));
    }

    public void readFields(DataInput dataInput) throws IOException {
        averageBytesPerRequest = Double.parseDouble(dataInput.readLine());
        totalBytes = Double.parseDouble(dataInput.readLine());
    }
}
