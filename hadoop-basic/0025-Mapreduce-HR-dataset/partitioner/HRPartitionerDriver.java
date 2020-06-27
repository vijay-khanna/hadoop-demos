package practiceMR;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;

public class HRPartitionerDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new Configuration(), new HRPartitionerDriver(), args);
        System.exit(returnStatus);
    }


public int run(String[] args) throws IOException{



	@SuppressWarnings("deprecation")
	Job birthday_summary_job = new Job(getConf());
	
	
	birthday_summary_job.setJobName("HR Birtdays Partitioner");
	
	birthday_summary_job.setJarByClass(HRPartitionerDriver.class);

	birthday_summary_job.setOutputKeyClass(Text.class);
	birthday_summary_job.setOutputValueClass(IntWritable.class);
	birthday_summary_job.setMapOutputKeyClass(Text.class);
	birthday_summary_job.setMapOutputValueClass(Text.class);
	 
	birthday_summary_job.setMapperClass(HRPartitionerMapper.class);
	birthday_summary_job.setReducerClass(HRPartitionerReducer.class);
	birthday_summary_job.setPartitionerClass(HRPartitionerPartitioner.class);
	birthday_summary_job.setNumReduceTasks(12); // Setting the total Reduce Tasks. .. could do this via CLI as well
	 
	FileInputFormat.addInputPath(birthday_summary_job, new Path(args[0]));
	FileOutputFormat.setOutputPath(birthday_summary_job,new Path(args[1]));
	   	
	
	try {
		return birthday_summary_job.waitForCompletion(true) ? 0 : 1;
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
