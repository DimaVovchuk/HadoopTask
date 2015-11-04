package com.lab.epam.hadoop;

import com.lab.epam.entity.Model;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by Bohdan-Dmytro_Vovchu on 11/4/2015.
 */
public class Reduce extends Reducer<Text, DoubleWritable, Text, Model> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        double averageSum = 0;
        int count = 0;
        for (DoubleWritable i: values) {
            sum += i.get();
            averageSum += i.get();
            count++;
        }
        averageSum /= count;
        context.write(key, new Model(sum,averageSum));
    }
}
