package com.lab.epam.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Bohdan-Dmytro_Vovchu on 11/4/2015.
 */
public class Map extends Mapper<LongWritable, Text, Text, Text> {
    private Text counter = new Text();
    private Text ip = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splitedLine = line.split(" ");
        try {
            ip.set(splitedLine[0]);
            counter.set(splitedLine[9]);
            context.write(ip,counter);
        }catch (IndexOutOfBoundsException e){

        }
    }
}
