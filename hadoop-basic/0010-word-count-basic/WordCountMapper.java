package com.wordcount;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
//Mapper inputs = Input Key Type, input Value Type, Output Key Type, Output Value Type
// E.g. for a file with content "Hello World, Hadoop is a great Technology"
// the input will be value of type text.
// (0, "Hello World, Hadoop ")
// (21,"is a great Technology")

		@Override
//override the default method in hadoop mapper class. Default map method is an identity function, it will write input key value pairs as they are without manipulating them.
// our map method has 3 input parameters
// 1. input key or byte offset
// 2. input value - line of Text
// 3. context. context class contains write method to be later used to write intermediate output

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {



		String st [] = value.toString().split("\\s+");
//split string based on spaces. store in st array.

        for(String st1 :  st) {
// loop throught array of words, and for each word, it writes a key value pair.
// key = word,
// value = constant integer 1
// the function converts words to lower case before writing, as the objective is to simply count the words regardless  of case.
// Text and IntWriteable constructirs is used to write key-value pairs

            context.write(new Text(st1.replaceAll("[^a-zA-Z]","").toLowerCase()), new IntWritable(1));
						// first iteration : (hello,1)
						// second iteration : (world,1)
        }

	}
}
