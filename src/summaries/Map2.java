package summaries;

/**
 * Created by hadoop on 29/04/2017.
 */

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map2 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //first get line in form of string
        String line = value.toString();

        //split up line to get the individual field
        String[] data = line.split(",");

        try {

            String gender = data[9];
            Double age = Double.parseDouble(data[4]);

            context.write(new Text(gender), new DoubleWritable(age));
        }
        catch (Exception e) {

        }

    }
}
