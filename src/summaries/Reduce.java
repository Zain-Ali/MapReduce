package summaries;

/**
 * Created by hadoop on 29/04/2017.
 */

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Collection;

public class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    public void reduce (final Text key,
                        final Iterable<DoubleWritable> values,
                        final Context context) throws IOException, InterruptedException {

        //sum divided by count to get average
        Double sum = 0.0;
        Integer count = 0;

        for (DoubleWritable value : values) {
            //iterate through all the values for the above line keys
            sum += value.get();
            count++;
        }

        Double ratio = sum / count;
        context.write(key, new DoubleWritable(ratio));
    }



}
