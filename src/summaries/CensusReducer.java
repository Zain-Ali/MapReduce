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

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Collection;

public class CensusReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private Double sum = 0.0;
    private Integer count = 0;

    @Override
    public void reduce (final Text key,
                        final Iterable<DoubleWritable> values,
                        final Context context) throws IOException, InterruptedException {

        for (DoubleWritable value : values) {
            sum += value.get();
            count++;
        }

        Double ratio = sum / count;
        context.write(key, new DoubleWritable(ratio));
        System.out.println("CensusReducer: Thread is " + Thread.currentThread().getId() + Thread.currentThread().getName());

    }
}
