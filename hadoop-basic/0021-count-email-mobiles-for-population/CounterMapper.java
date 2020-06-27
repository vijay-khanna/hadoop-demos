package countMobileAndEmailMembers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

/**
* 0,1,2,3,4,5,6,7,8,9,10,11
* Registrar,Enrolment Agency,State,District,Sub District,Pin Code,Gender,Age,Aadhaar generated,Enrolment Rejected,Residents providing email,Residents providing mobile number
* Allahabad Bank,A-Onerealtors Pvt Ltd,Uttar Pradesh,Allahabad,Meja,212303,F,7,1,0,0,1
10th index gives 0, if none of the applicants in the area did gave email.
11th index gives 0, if none of the applicants in the area did gave mobile.

the 10th and 11th indexes count the total people who provided emails and mobile numbers
 *
 */
public class CounterMapper extends
    Mapper<LongWritable, Text, Text, IntWritable> {
	enum InfoCounter{EMAIL,MOBILE};
  // using the java Enumerator class here.  Object Infocounter with fields named email and mobile. The group is called Infocounter, with two fields-values.

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] fields = value.toString().split(",");//split each line as per comma delimiter.
    if (fields.length > 1) {
      int email = Integer.parseInt(fields[10]);//get from 10th index in CSV.
      int mobile = Integer.parseInt(fields[11]);
      if (email==1) {
          context.getCounter(InfoCounter.EMAIL).increment(1);
          //increment the InfoCounter if value is non-zero.
          //not writing anything on disk in this mapper class. This will just display at end of execution.
          // Checks the count of areas where people provided more than one emails and increments the total count.
       }
      if (mobile==1) {
    	  context.getCounter(InfoCounter.MOBILE).increment(1);
      }
    }
  }
}
