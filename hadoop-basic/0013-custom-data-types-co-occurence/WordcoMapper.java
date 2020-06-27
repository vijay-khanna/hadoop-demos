/*
 * Create custom object. it will be used as key or value in MR program.
 * need to implement particular interfaces. 
 * If it needs to be used as a key, then it needs to definitely implement WritableComparable Interface. 
 * If we are simply using it as value, then we need not implement WritableComparable, but only implement the Writable interface. 
 * This for intermediate shuffle and sort. For comparing keys in intermediate key-value pairs. 
 * It will group them by keys, and send them to relevant reducer. 
 * 
 * 
 * Need to tell the framework how to compare the custom objects. 
 * 
 * 
 * ##Problem Statement here :  * create word co-occurence matrix.  
 * this is used in Natural Language processing domain. E.g. When buying a phone, check for screen guard to co-sell
 * 
 *e.g. // this is a simple program to understand custom object 
 *  > Q: count frequency of each pair in the document
 *  either Make use of immediate pairs, e.g. This is, is a, a sample, 
 *  or set number of neighbours : i.e. This is, This a, This sample.... //the neighbours can be set in program.
 * 
 *  in Mapper phase :  for each pair, emit a constant = 1. 
 *  in Reducer phase : for each pairs, aggregate the values to get frequency. 
 *  
 *  
 */


package wordCoOccurence;
import org.apache.hadoop.mapreduce.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;



public class WordcoMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {
    private TextPair wordPair = new TextPair();
    private IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int neighbors = context.getConfiguration().getInt("neighbors", 2);
        // can specify the number of neighbors using Command line -D option.  
        
        String[] tokens = value.toString().split("\\s+"); //splitting each line as per white space
        
        if (tokens.length > 1) {
          for (int i = 0; i < tokens.length; i++) {
              wordPair.setFirst(new Text(tokens[i]));        // setting the first word.. this outer for loop, and for each word, there will be inner loop as far as neighbour parameter value. 
              

             int start = (i - neighbors < 0) ? 0 : i - neighbors;
             int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
              for (int j = start; j <= end; j++) {
                  if (j == i) continue;
                   wordPair.setSecond(new Text(tokens[j])); // second word in the pair. 
                   
                   context.write(wordPair, ONE); //write a constant 1 as value for each pair.
              }
          }
      }
  }
}

class TextPair implements WritableComparable<TextPair> {
	 //Need to Implement WritableComparable, as it will be used as key. 
	
	
    private Text first;
    private Text second;
 
    public TextPair(Text first, Text second) {
        setFirst(first);
        setSecond(second);
    }
 
    public TextPair() {
        setFirst(new Text());
        setSecond(new Text());
    }
 
    public TextPair(String first, String second) {
        setFirst(new Text(first)); //Textboxing. 
        setSecond(new Text(second));
    }
 
    public Text getFirst() {
        return first;
    }
 
    public Text getSecond() {
        return second;
    }
 
    public void setFirst(Text first) {
        this.first = first;
    }
    
    public void setSecond(Text second) {
    	this.second=second;
    }
 
    public void readFields(DataInput in) throws IOException {
        first.readFields(in); // readFields is already implemented in Text Class. 
        second.readFields(in);
    }
 
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }
 
    @Override
    public String toString() {
        return first + " " + second; //return the word-pair, space-delimited. 
    }
 
    public int compareTo(TextPair tp_text_pair_two_words) {
    	//compares first field of your object with the first field of the object passed to
        int cmp_comparison_result_of_two_words = first.compareTo(tp_text_pair_two_words.first);
 
        if (cmp_comparison_result_of_two_words != 0) { // if they are different return result. 
            return cmp_comparison_result_of_two_words;
        }
 
        return second.compareTo(tp_text_pair_two_words.second); //if first fields of both objects are equal, i.e. cmp = 0, then compare second field and return the result
        
    }
 
    @Override
    public int hashCode(){
        return first.hashCode()*163 + second.hashCode();
        // to ensure there is no ambiguity in results. if two objects are equal then ensure the hashcodes are equal.
        // first multiplied by a prime number
    }
 
    @Override
    public boolean equals(Object o)
    {
    	//need to convert the object to TextPair. and return 
        if(o instanceof TextPair)
        {
            TextPair tp = (TextPair) o;
            return first.equals(tp.first) && second.equals(tp.second);
            //return only if this condition is met. 
        }
        return false;
    }

    
 
}

