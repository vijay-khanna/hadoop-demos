package join;
//  File 1 = Transactions.csv. 	: User_ID, Product_ID 
//	File 2 = Products.csv. 		: Product_ID, Product_Name
//  List the user ID, and product name which they purchased using Join,



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MrjoinDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new Configuration(), new MrjoinDriver(), args);
        System.exit(returnStatus);
    }


public int run(String[] args) throws IOException{



	@SuppressWarnings("deprecation")
	Job job = new Job(getConf());
	
	
	 job.setJobName("Mapreduce join");
	
	 job.setJarByClass(MrjoinDriver.class);

	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(Text.class);
	 job.setMapOutputKeyClass(Text.class);
	 job.setMapOutputValueClass(Text.class);
	 job.setReducerClass(MrjoinReducer.class);
	
	
	 MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, FirstMapper.class);
	 
	 MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, SecondMapper.class);
	 // AddInputPath api can work with multiple arguments.can pass the job to specific Mapperclass. 
	 // pass each input file to different mapper class. 
	 
	 FileOutputFormat.setOutputPath(job,new Path(args[2]));
	 
//      args[0] File 1 = Transactions.csv. 	: User_ID, Product_ID 
//		args[1] File 2 = Products.csv. 		: Product_ID, ProductName
	
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

