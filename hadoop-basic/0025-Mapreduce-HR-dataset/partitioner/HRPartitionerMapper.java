package practiceMR;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.logging.Level;
import java.util.logging.Logger;


/*
 * make month-wise files containing employee names, employee IDs and birthday without the year and the month.
* solve the problem by writing a mapreduce program that uses a custom data type containing employee name and ID
* implement your own partitioner to ensure that the employee details of the employees with birthdays in the same month go to the same reducer.
 *
 *employee_name = 
 *employee_id = 
 *birthday = 
 *
 *Birthday Format : 

* 0		Employee Name	Brown, Mia
* 1		Employee Number	1103024456
* 12	DOB				11/24/1987			mm/dd/yyyy

 *
 */


public class HRPartitionerMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final Logger LOG = Logger.getLogger(HRPartitionerMapper.class.getName());
	public static List<String> months = Arrays.asList("1","2","3","4","5","6","7","8","9","10","11","12");
	// 1 = Jan, 12 = Dec
	

 
  @Override
  public void map(LongWritable key, Text individual_record_value, Context context)
      throws IOException, InterruptedException {
    
    String[] employee_record_array = individual_record_value.toString().split(","); //split the line based on spaces, and store in array. 
    //
    String log_message = "* * * * Raw Value line : "+individual_record_value.toString();
    LOG.log(Level.INFO, log_message);
    
    
    
    if (employee_record_array.length > 3) { /// This checks if there is no junk, and it is a valid sized field
      String employee_name = employee_record_array[0]; // Get the name
      String employee_id = employee_record_array[1]; // Get the name
      System.out.println(" employee_name : "+employee_name);
      String[] employee_dob_array = employee_record_array[12].split("/"); //Split the Date field, 
      
      log_message = "* * * * * * * * Employee ID : "+employee_name;
      LOG.log(Level.INFO, log_message);
      
      if (employee_dob_array.length > 1) {
        String dob_theMonth_Value = employee_dob_array[0]; //0th field is month
        
        
     
        String employee_name_id = employee_name+" "+employee_id;
        System.out.println("* * * * * dob-month-value --check "+dob_theMonth_Value+"  employee Details : "+employee_name_id);
        if (months.contains(dob_theMonth_Value))
        	System.out.println("* * * * * dob-month-value found"+dob_theMonth_Value+"  employee Details : "+employee_name_id);
        	context.write(new Text(employee_name_id), new Text(dob_theMonth_Value));
        //employee_name_id ,,dob_theMonth_Value
        
        //	context.write(new Text(source_ip_Key), new Text(theMonth_Value)); /
      }
    }
  }
}
