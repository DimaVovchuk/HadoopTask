package com.lab.epam.mrunit;

import com.lab.epam.entity.Model;
import com.lab.epam.hadoop.Map;
import com.lab.epam.hadoop.Reduce;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Bohdan-Dmytro_Vovchu on 11/5/2015.
 */
public class Tests {

    private final static String value = "ip1 - - [24/Apr/2011:04:06:01 -0400] GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1 200 111 - Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)";
    private final static String contex = "111";
    private final static String key = "ip1";

    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Model> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, Text, Text, Model> mapReduceDriver;

    @Before
    public void setup() {
        Map mapper = new Map();
        Reduce reducer = new Reduce();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(value));
        mapDriver.withOutput(new Text(key), new Text(contex));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<Text> values = new ArrayList<>();
        values.add(new Text(contex));
        values.add(new Text(contex));
        DoubleWritable averageBytesPerRequest = new DoubleWritable(111);
        DoubleWritable sum = new DoubleWritable(222);
        reduceDriver.withInput(new Text(key), values);
        reduceDriver.withOutput(new Text(key), new Model(averageBytesPerRequest, sum));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        DoubleWritable averageBytesPerRequest = new DoubleWritable(111);
        DoubleWritable sum = new DoubleWritable(333);

        mapReduceDriver.withInput(new LongWritable(), new Text(value)).withInput(new LongWritable(),
                new Text(value)).withInput(new LongWritable(), new Text(value)).withReducer(
                new Reduce()).withOutput(new Text(key), new Model(averageBytesPerRequest, sum));

        mapReduceDriver.runTest();
    }
}
