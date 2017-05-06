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
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlagMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //first get line in form of string
        String line = value.toString();

        //split up line to get the individual field
        String[] data = line.split(",");

        try {

            String countryName = data[0];
            Double listOfCities = Double.parseDouble(data[3]);

            System.out.println("Thread mapper2 "  + Thread.currentThread().getId());
            context.write(new Text(countryName), new DoubleWritable(listOfCities));
        }
        catch (Exception e) {

        }
    }
}
