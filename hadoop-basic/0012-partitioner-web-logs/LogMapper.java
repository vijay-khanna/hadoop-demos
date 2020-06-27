package com.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<LongWritable, Text, Text, Text> {

	public static List<String> months = Arrays.asList("Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec");

  /**
   * Example input line:
   * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /flight.jpg HTTP/1.1" 200 12776
   * 
   * Count # Requests per IP, and Classified as per Month wise
   * Create 12 different WebLogs, each corresponding to a month of year. e.g. first file will have records for jan, next for feb..
   * Key will be IP_Address, Value = Month
   * Without custom partitioner, we dont know if it will give us data in month wise files
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String[] fields_array_weblog_line = value.toString().split(" "); //split the line based on spaces, and store in array. 
    //
    
    if (fields_array_weblog_line.length > 3) { /// This checks if there is no junk, and it is a valid sized field
      String source_ip_Key = fields_array_weblog_line[0]; // Get the IP
      String[] DateTimeField = fields_array_weblog_line[3].split("/"); //Split the DateTime field, and pick the 3rd index to get Month
      if (DateTimeField.length > 1) {
        String theMonth_Value = DateTimeField[1];
     
        if (months.contains(theMonth_Value))
        	context.write(new Text(source_ip_Key), new Text(theMonth_Value)); //Writes in format <IP,Mon> . e.g. 10.10.10.1 Apr
        
      }
    }
  }
}
