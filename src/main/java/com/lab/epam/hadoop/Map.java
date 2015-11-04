package com.lab.epam.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * Created by Bohdan-Dmytro_Vovchu on 11/4/2015.
 */
public class Map extends Mapper<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable counter = new DoubleWritable();
    private Text ip = new Text();

    @Override
    protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splitedLine = line.split(" ");
        ip.set(splitedLine[0]);
        counter.set(Integer.parseInt(splitedLine[8]));
        context.write(ip,counter);
    }
}
