package com.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;

public class LogDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new Configuration(), new LogDriver(), args);
        System.exit(returnStatus);
    }


public int run(String[] args) throws IOException{



	@SuppressWarnings("deprecation")
	Job ip_summary_job = new Job(getConf());
	
	
	ip_summary_job.setJobName("Web Log Partitioner");
	
	ip_summary_job.setJarByClass(LogDriver.class);

	ip_summary_job.setOutputKeyClass(Text.class);
	ip_summary_job.setOutputValueClass(IntWritable.class);
	ip_summary_job.setMapOutputKeyClass(Text.class);
	ip_summary_job.setMapOutputValueClass(Text.class);
	 
	ip_summary_job.setMapperClass(LogMapper.class);
	ip_summary_job.setReducerClass(LogReducer.class);
	ip_summary_job.setPartitionerClass(LogPartitioner.class);
	ip_summary_job.setNumReduceTasks(12); // Setting the total Reduce Tasks. .. could do this via CLI as well
	 
	FileInputFormat.addInputPath(ip_summary_job, new Path(args[0]));
	FileOutputFormat.setOutputPath(ip_summary_job,new Path(args[1]));
	   	
	
	try {
		return ip_summary_job.waitForCompletion(true) ? 0 : 1;
	} catch (ClassNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	return 0;
	
	
  
}


}
