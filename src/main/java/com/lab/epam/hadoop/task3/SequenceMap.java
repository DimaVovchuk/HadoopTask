package com.lab.epam.hadoop.task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Bohdan-Dmytro_Vovchu on 11/6/2015.
 */
public class SequenceMap extends Mapper<LongWritable, Text, Text, Text> {
    private Text ip = new Text();
    private Text browser = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splitedLine = line.split(" ");
        try {
            ip.set(splitedLine[0]);
            browser.set(splitedLine[11]);
            long totalBytes = Long.parseLong(splitedLine[9]);
            context.write(ip, browser);
        } catch (IndexOutOfBoundsException | NumberFormatException e) {

        }
    }
}
