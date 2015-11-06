package com.lab.epam.hadoop;

import com.lab.epam.entity.Model;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by Bohdan-Dmytro_Vovchu on 11/4/2015.
 */
public class Reduce extends Reducer<Text, Model, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Model> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        long averageSum = 0;
        int count = 0;
        for (Model model : values) {
            try {
                sum += model.getTotalBytes();
                count += model.getCount();
            } catch (NumberFormatException e) {
            }
        }
        if(count != 0){
            averageSum = sum / count;
        }
        context.write(key, new Text(String.valueOf(averageSum)+"," + String.valueOf(sum)));
    }
}
