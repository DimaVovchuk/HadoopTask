package com.lab.epam.hadoop.task2;

import com.lab.epam.entity.Model;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Bohdan-Dmytro_Vovchu on 11/5/2015.
 */
public class Combiner extends Reducer<Text, Text, Text, Model> {

    public void reduce(Text key, Iterable<Model> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        int count = 0;
        for (Model model : values) {
            try {
                sum += model.getTotalBytes();
                count += model.getCount();
            } catch (NumberFormatException e) {
            }
        }
        context.write(key, new Model(sum, count));
    }

}
