package com.lab.epam.hadoop;

import com.lab.epam.entity.Model;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by Bohdan-Dmytro_Vovchu on 11/4/2015.
 */
public class Reduce extends Reducer<Text, Text, Text, Model> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        double averageSum = 0;
        int count = 0;
        for (Text i: values) {
            try {
                sum += Double.parseDouble(String.valueOf(i));
                averageSum += Double.parseDouble(String.valueOf(i));
                count++;
            } catch (NumberFormatException e){

            }
        }
        averageSum /= count;
        context.write(key, new Model(sum,averageSum));
    }
}
