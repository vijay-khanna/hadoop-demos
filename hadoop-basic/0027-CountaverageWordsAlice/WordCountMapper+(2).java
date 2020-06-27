package alicewordCountAverage;

import java.io.IOException;


import java.util.logging.Level;
import java.util.logging.Logger;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
@SuppressWarnings({ "unused", "deprecation" })
public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private static final Logger LOG = Logger.getLogger(WordCountMapper.class.getName());
	
	//input=(byte offset, text) output=(word, 1)
	@Override
	
	
	public void map(LongWritable key, Text novel_section_value, Context context) throws IOException, InterruptedException {
		
		
    	
 //The course 2 of = ["The", "course", "2", "of"] 
		String string_array [] = novel_section_value.toString().split("\\s+");
		
		
        for(String each_word :  string_array) {
        	
        	
        	String temp_clean_word = each_word.replaceAll("[\\!\\-\\+\\.\\^:,]","");
        	
        	String temp_clean_word_filtered = temp_clean_word.replaceAll("[-+.^:,]","");
        	
        	String lowerCase_eachword = temp_clean_word_filtered.replaceAll("[^a-zA-Z]+","").toLowerCase();
        	if ( lowerCase_eachword.length()>=1) {
        		char first_letter = lowerCase_eachword.charAt(0);
            	
            	String str_firstCharacterOfWord = String.valueOf(first_letter);
            	        	
            	int len_word = lowerCase_eachword.length(); 
            	
            	String out = "* * * : "+str_firstCharacterOfWord + "..str_firstCharacterOfWord ."+ "...len_word: "+len_word+"..lowerCase_eachword.."+lowerCase_eachword;
            	LOG.log(Level.INFO, out);
            	            	
                context.write(new Text(str_firstCharacterOfWord), new IntWritable(len_word)); 
        	}
        	
         
        }

	}
}
