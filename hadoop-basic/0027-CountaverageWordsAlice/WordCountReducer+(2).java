package alicewordCountAverage;

import java.io.*;

//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.logging.Level;
import java.util.logging.Logger;
//import org.apache.hadoop.mapred.lib.MultipleOutputs;
 

//public class WordCountReducer extends Reducer<Text, IntWritable, Text, Text>{
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	private static final Logger LOG = Logger.getLogger(WordCountReducer.class.getName());
	
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException {
  //("the",[1,1,1,1])
        int countOfWords = 0;
        int total_length = 0;
        
        
        for(IntWritable lengthOfEachWord : values) {
        	total_length = total_length + lengthOfEachWord.get();
        	countOfWords = countOfWords + 1;
        	
         //("the",[1,1,1,1]) --> ("the", 4)  
        }
        
        
        try {
        	int  average_length_perKey = total_length/countOfWords;
        	String out = "* * * : "+key + "..."+ "..Total Words :"+countOfWords + ".......Total Character Length : "+total_length+ ".......Average Word Length : "+average_length_perKey;
        	LOG.log(Level.INFO, out);
        	
			context.write(key, new IntWritable(average_length_perKey));
        	//		context.write(key, new Text(out));
        
		} catch (InterruptedException e) {
		
			e.printStackTrace();
		}     
	
    }
}