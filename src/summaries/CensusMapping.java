package summaries;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import summaries.Main;
import summaries.Map;
import summaries.Reduce;

/**
 * Created by hadoop on 04/05/2017.
 */
public class CensusMapping extends Thread {

    private Configured conf;
    public CensusMapping(Configured conf){
        this.conf = conf;
    }

    public void run(){
        try {
            job();
        }
        catch(Exception ex){

        }
    }

    public void job() throws Exception{

        Job job = Job.getInstance(conf.getConf());
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
        job.waitForCompletion(true);
    }

}
