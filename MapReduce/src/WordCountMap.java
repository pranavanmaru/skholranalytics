import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by pranavanmaru on 5/4/17.
 */
public class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");

        for(String word: words){

            context.write(new Text(word), new IntWritable(1));
        }
    }
}
