/**
 * Author: UP687776
 *
 * The code was original taken from
 * https://app.pluralsight.com/library/courses/mapreduce-applying-common-data-problems/table-of-contents
 * https://app.pluralsight.com/library/courses/mapreduce-applying-common-data-problems/exercise-files
 *
 * However, it has been significantly changed and refactored and it’s not in its original state
 */

package summaries;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlagJob extends Thread {

    private Configured conf;
    public FlagJob(Configured conf){
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

        job.setMapperClass(FlagMapper.class);
        job.setReducerClass(FlagReducer.class);

        Path inputFilePath1 = new Path("/Users/hadoop/IdeaProjects/MapReduce/data/input/flags.txt");
        Path outputFilePath1 = new Path("/Users/hadoop/IdeaProjects/MapReduce/data/output1");

        FileInputFormat.addInputPath(job, inputFilePath1);
        FileOutputFormat.setOutputPath(job, outputFilePath1);
        job.submit();
    }
}
