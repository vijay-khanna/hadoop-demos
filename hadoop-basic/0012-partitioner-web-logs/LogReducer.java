package com.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Counts the number of values associated with a key */

public class LogReducer extends Reducer<Text, Text, Text, IntWritable> {

	@Override
	public void reduce(Text ip_address_key, Iterable<Text> ip_values_list, Context context)
			throws IOException, InterruptedException {


		int count_of_ips = 0;

		for (@SuppressWarnings("unused")
		Text single_ip_count : ip_values_list) {
			count_of_ips++;
			//iterate the list
			//adds the occurences of each ip to count_of_ips
		}

		context.write(ip_address_key, new IntWritable(count_of_ips)); // writes summary result <IP> <count_of_ips>. 
		
	}
}
