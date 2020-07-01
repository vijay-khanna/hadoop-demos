package wordCoOccurence;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

import org.apache.hadoop.io.*;

public class WordcoReducer extends Reducer<TextPair,IntWritable,TextPair,IntWritable> {
    private IntWritable totalCount = new IntWritable();
    @Override
    protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
             count += value.get(); //aggregate the sum of 1's to get frequency
        }
        totalCount.set(count);
        context.write(key,totalCount);
    }
}


