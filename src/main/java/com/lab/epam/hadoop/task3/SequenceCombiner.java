package com.lab.epam.hadoop.task3;

import com.lab.epam.entity.Model;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Bohdan-Dmytro_Vovchu on 11/6/2015.
 */
public class SequenceCombiner extends Reducer<Text, Text, Text, Model> {
    public void reduce(Text key, Iterable<Model> values, Context context) throws IOException, InterruptedException {

    }
}
