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

public class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //first get line in form of string
        String line = value.toString();

        //split up line to get the individual field
        String[] data = line.split(",");

        try {
            //find their martial status
            String martialStatus = data[5];

            //number of hours these individuals work
            Double hrs = Double.parseDouble(data[12]); //12

            //output the data
            context.write(new Text(martialStatus), new DoubleWritable(hrs));
        }
        catch (Exception e) {

        }

        try {

            String jobType = data[1];
            Double age = Double.parseDouble(data[4]);

            context.write(new Text(jobType), new DoubleWritable(age));
        }
        catch (Exception e) {

        }

    }
}
