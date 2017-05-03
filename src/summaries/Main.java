package summaries;

/**
 * Created by hadoop on 29/04/2017.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class Main extends Configured implements Tool {

    @Override
    public int run (String[] args) throws Exception {

        //Job One
        Job job = Job.getInstance(getConf());
        job.setJobName("average");
        job.setJarByClass(Main.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        Path inputFilePath = new Path("/Users/hadoop/IdeaProjects/MapReduce/data/input/census.txt");
        Path outputFilePath = new Path("/Users/hadoop/IdeaProjects/MapReduce/data/output");

        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);

        job.waitForCompletion(true);// ? 0 : 1;

//        Job Two
        Job job2 = Job.getInstance(getConf());
        job2.setJobName("age");
        job2.setJarByClass(Main.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);

        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reduce2.class);

        Path inputFilePath1 = new Path("/Users/hadoop/IdeaProjects/MapReduce/data/input/census.txt");
        Path outputFilePath1 = new Path("/Users/hadoop/IdeaProjects/MapReduce/data/output1");

        FileInputFormat.addInputPath(job2, inputFilePath1);
        FileOutputFormat.setOutputPath(job2, outputFilePath1);

        return  job2.waitForCompletion(true) ? 0 : 1;

    }


    public static void main (String[] args) throws Exception {

        Long startTime = System.currentTimeMillis();

        int exitCode = ToolRunner.run(new Main(), args);

        Long endTime = System.currentTimeMillis();
        System.out.println("Calculated in " + (endTime - startTime) + " milliseconds");
    }
}



