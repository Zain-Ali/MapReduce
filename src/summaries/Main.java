package summaries;

/**
 * Created by hadoop on 29/04/2017.
 */

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class Main extends Configured implements Tool {

    @Override
    public int run (String[] args) throws Exception {
        CensusMapping cs = new CensusMapping(this);
        FlagsMapping fs = new FlagsMapping(this);

        //single threaded
//        cs.job();
//        fs.job();

        //multi threaded
        cs.start();
        fs.start();

        cs.join();
        fs.join();
        return 0;
    }

    public static void main (String[] args) throws Exception {

        Long startTime = System.currentTimeMillis();

        int exitCode = ToolRunner.run(new Main(), args);

        Long endTime = System.currentTimeMillis();
        System.out.println("Calculated in " + (endTime - startTime) + " milliseconds");
    }
}