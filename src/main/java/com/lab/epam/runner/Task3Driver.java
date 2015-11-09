package com.lab.epam.runner;

import com.lab.epam.entity.Model;
import com.lab.epam.entity.ResultModel;
import com.lab.epam.hadoop.task2.Combiner;
import com.lab.epam.hadoop.task2.Map;
import com.lab.epam.hadoop.task2.Reduce;
import com.lab.epam.hadoop.task3.SequenceCombiner;
import com.lab.epam.hadoop.task3.SequenceMap;
import com.lab.epam.hadoop.task3.SequenceReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Bohdan-Dmytro_Vovchu on 11/6/2015.
 */
public class Task3Driver extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf);
        job.setJarByClass(Task3Driver.class);
        Path in = new Path(strings[0]);
        Path out = new Path(strings[1]);
        job.setJobName("IP traffic counter");

        job.setMapperClass(SequenceMap.class);
        //job.setCombinerClass(SequenceCombiner.class);
        job.setReducerClass(SequenceReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Task3Driver(), args);
        System.exit(res);
    }
}
