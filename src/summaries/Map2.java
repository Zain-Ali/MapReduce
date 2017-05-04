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

            String countryName = data[0];
            Double listOfCities = Double.parseDouble(data[3]);

            String flagColour = data[29];
            Double countryCode = Double.parseDouble(data[27]);


            context.write(new Text(countryName), new DoubleWritable(listOfCities));
            context.write(new Text(flagColour), new DoubleWritable(countryCode));
        }
        catch (Exception e) {

        }

    }
}
