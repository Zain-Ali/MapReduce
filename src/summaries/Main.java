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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

    @Override
    public int run (String[] args) throws Exception {
        CensusJob cs = new CensusJob(this);
        FlagJob fs = new FlagJob(this);

        //single threaded
//        cs.job();
//        fs.job();

//        //multi threaded
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