package practiceMR;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* Counts the number of values associated with a key */

public class HRPartitionerReducer extends Reducer<Text, Text, Text, IntWritable> {

	@Override
	public void reduce(Text employee_name_id_key, Iterable<Text> employee_name_id_list, Context context)
			throws IOException, InterruptedException {


		int count_of_employees = 0;

		for (@SuppressWarnings("unused")
		Text each_employee_name_id : employee_name_id_list) {
			count_of_employees++;
			//iterate the list
			//adds the occurences of each ip to count_of_ips
		}

		context.write(employee_name_id_key, new IntWritable(count_of_employees)); // writes summary result <IP> <count_of_ips>. 
		//mapper = 	context.write(new Text(source_ip_Key), new Text(theMonth_Value));
		//reducer = 		context.write(ip_address_key, new IntWritable(count_of_ips)); 
	}
}
