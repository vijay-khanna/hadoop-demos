package alicewordCountAverage;

/*
 * Input File format
 * Name, Gender, Salary
John,M,10000
Anita,F,50000
George,M,4000
Maven,F,35000
Tika,M,2000
Bina,F,3500
 * 
 * 
hadoop fs -cat /tmp/hadoop_jobs/salary/output/*
F       Total: 88500.0 :: Average: 29500.0
M       Total: 16000.0 :: Average: 5333.3335

 * Calculate the average salary per gender type
 * 
 */
import java.io.IOException;
//import java.util.logging.Level;
import java.util.logging.Logger;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AveragewordsAliceAlphabetically
{

	public static class MapperClass extends Mapper<Object, Text, Text, FloatWritable>
	{

	//	private static final Logger LOG = Logger.getLogger(<YOURMAPPERCLASSNAME>.class.getName());
		private static final Logger LOG = Logger.getLogger(MapperClass.class.getName());

 		public void map(Object key, Text novel_portion, Context con)
			throws IOException, InterruptedException
		{
  			String[] word = novel_portion.toString().split(" ");
  			String gender = word[1];
   			Float salary = Float.parseFloat(word[2]);
   		//	String log_message = "**** Check Status : Gender  "+ gender + " Salary :"+salary;
   		//	LOG.log(Level.INFO, log_message);
   		//	LOG.log(Level.INFO, "* * Test Mapper");

   			con.write(new Text(gender), new FloatWritable(salary));
 		}
	}





	public static class ReducerClass extends Reducer<Text, FloatWritable, Text, Text>
	{
		private static final Logger LOG = Logger.getLogger(ReducerClass.class.getName());
 		public void reduce(Text key, Iterable<FloatWritable> valueList,
			Context con) throws IOException, InterruptedException
		{
   			float total = (float) 0;
   			int count = 0;
   			for (FloatWritable var : valueList)
			{
    				total += var.get();
    				count++;
   			}
   			float avg = total / (float)count;
   			String out = "Total: " + total + " :: " + "Average: " + avg;
   		//	String log_message = "**** Check ";
   		//	LOG.log(Level.INFO, "* * Test Reducer");
   		//	LOG.log(Level.INFO, log_message);
   			
   			con.write(key, new Text(out));
 		}
	}

	public static void main(String[] args) throws Exception
	{
    		Configuration conf = new Configuration();
    		Job job = Job.getInstance(conf, "avgAndTotalSalaryCompute");
    		job.setJarByClass(AveragewordsAliceAlphabetically.class);
		job.setMapperClass(MapperClass.class);
    		//job.setCombinerClass(IntSumReducer.class);
    		job.setReducerClass(ReducerClass.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

    		//job.setOutputKeyClass(IntWritable.class);
    		//job.setOutputValueClass(Text.class);
    		FileInputFormat.addInputPath(job, new Path(args[0]));
    		FileOutputFormat.setOutputPath(job, new Path(args[1]));
    		System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
}
