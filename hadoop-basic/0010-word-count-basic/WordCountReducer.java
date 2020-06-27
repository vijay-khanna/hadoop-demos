package com.wordcount;

import java.io.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
// extends Hadoop reducer class
// Input of Reducer should match the output of mapper.
//Before feeding data to Reducers, system shuffles and sorts and creates a list of values associated with each key output from mappers.
// the Input value to reducer is an itterable list consisting of all values corresponding to a given key
// The Output key and value must align with the objective of printing each word and its frequency => Text(Key) and IntWritable(Value)
// sample input to the Reducer phase("the",[1,1,1,1])

//the Reduce method is called once for each Key.  the Default Reduce method is an identity function.

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException {
        int count = 0;

        for(IntWritable val : values) {
            count = count + val.get();     // get() method to get the integer value.
// ("the",[1,1,1,1]) => ("the",4)             )

        }

        try {
			context.write(key, new IntWritable(count));   //using write method of context class,  write the final count on HDFS

		} catch (InterruptedException e) {

			e.printStackTrace();
		}

    }
}
