package alicewordCountAverage;

import java.io.*;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;


 
@SuppressWarnings("unused")
public class WordCount extends Configured implements Tool{
	 
	
	public static void main(String[] args) throws Exception {
	        int returnStatus = ToolRunner.run(new Configuration(), new WordCount(), args);
	        System.exit(returnStatus);
	    }

    
	public int run(String[] args) throws IOException{
   
  
  
    	@SuppressWarnings("deprecation")
		Job job = new Job(getConf());
    	
    	
    	 job.setJobName("Word Count");
    	
    	 job.setJarByClass(WordCount.class);

    	 job.setOutputKeyClass(Text.class);
    	 job.setOutputValueClass(IntWritable.class);
    	 
    	 job.setMapperClass(WordCountMapper.class);
    	 job.setCombinerClass(WordCountReducer.class);
    	 job.setReducerClass(WordCountReducer.class);
    	
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job,new Path(args[1]));
    	   	
    	
    	try {
			return job.waitForCompletion(true) ? 0 : 1;
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