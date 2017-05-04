package summaries;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by hadoop on 04/05/2017.
 */
public class FlagsMapping extends Thread {

    private Configured conf;
    public FlagsMapping(Configured conf){
        this.conf = conf;
    }

    public void run(){
        try {
            job();
        }catch(Exception ex){
            
        }
    }

    public void job() throws Exception{

        Job job = Job.getInstance(conf.getConf());
        job.setJobName("age");
        job.setJarByClass(Main.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map2.class);
        job.setReducerClass(Reduce2.class);

        Path inputFilePath1 = new Path("/Users/hadoop/IdeaProjects/MapReduce/data/input/flags.txt");
        Path outputFilePath1 = new Path("/Users/hadoop/IdeaProjects/MapReduce/data/output1");

        FileInputFormat.addInputPath(job, inputFilePath1);
        FileOutputFormat.setOutputPath(job, outputFilePath1);
        job.waitForCompletion(true);
    }

}
