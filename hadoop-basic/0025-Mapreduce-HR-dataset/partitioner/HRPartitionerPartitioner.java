package practiceMR;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class HRPartitionerPartitioner extends Partitioner<Text,Text > implements
    Configurable {

  private Configuration configuration;
  HashMap<String, Integer> monthsHashMap = new HashMap<String, Integer>();

  /**
   * Set up the months hash map in the setConf method.
   */
  public void setConf(Configuration configuration) {
	  //Simple Key Value hashMap
	  //Jan months records will go to Reducer #0
	  //setConf needs to be implemented in configurable interface.
	  // Populating the hashMap Manually. 
	  
    this.configuration = configuration;
    monthsHashMap.put("1", 0);
    monthsHashMap.put("2", 1);
    monthsHashMap.put("3", 2);
    monthsHashMap.put("4", 3);
    monthsHashMap.put("5", 4);
    monthsHashMap.put("6", 5);
    monthsHashMap.put("7", 6);
    monthsHashMap.put("8", 7);
    monthsHashMap.put("9", 8);
    monthsHashMap.put("10", 9);
    monthsHashMap.put("11", 10);
    monthsHashMap.put("12", 11);
  }

  /**
   * Implement the getConf method for the Configurable interface.
   */
  public Configuration getConf() {
	    // Default implementation, as we are implementing an interface, so need to implement all the methods it supports.
	  return configuration;

  }

  /**
   * You must implement the getPartition method for a partitioner class.
   * This method receives the three-letter abbreviation for the month
   * as its value. (It is the output value from the mapper.)
   * It should return an integer representation of the month.
   * Note that January is represented as 0 rather than 1.
   * 
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly 12 reducers.
   */
  public int getPartition(Text key, Text month_value_In_English, int numberOfReduceTasks) {
	  // for every intermediate key-value pair this method will be called.
	  // it will be passed the key and value emitted by the mapper. 
	  // need to set the numberOfReduceTasks to 12 manually / via configuration parameter. 
	  
	  
	  
    return (int) (monthsHashMap.get(month_value_In_English.toString()));
// the int returned by this method is the value (Month Index) corresponding to they value against the key (Month Name e.g Apr)
  
  }
}
