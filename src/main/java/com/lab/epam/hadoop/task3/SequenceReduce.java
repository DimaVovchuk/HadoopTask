package com.lab.epam.hadoop.task3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Bohdan-Dmytro_Vovchu on 11/6/2015.
 */
public class SequenceReduce extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<Text> set = new HashSet<>();
        while (values.iterator().hasNext()) {
            set.add(values.iterator().next());
        }

        for (Text text : set) {
            context.write(key, text);
            context.getCounter("browser", text.toString()).increment(1);
        }

    }
}
