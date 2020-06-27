package join;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

import org.apache.hadoop.io.*;

/*
 *  File 1 = transactions.csv. 	: User_ID, Product_ID
 *	File 2 = products.csv. 	  	: Product_ID, Product_Name
 *  List the user ID, and product name which they purchased using Join,
 * 
 * 
 * *** This Second Mapper handles the products.csv file/table.  
 * 0th col = product_ID
 * 1st col = Product_Name
 * 
 *  * 
 * in transactions record write function : //key = product_id .value = "TransactionRecord_FirstTable <tab> user_id"
 * in products record write function 	: //key = product_id .Value = "ProductRecord_SecondTable <tab> product_name" 
 */


public class SecondMapper extends Mapper<LongWritable,Text,Text,Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
	String record = value.toString();
	String[] parts = record.split(",");
	context.write(new Text(parts[0]), new Text("ProductRecord_SecondTable\t" + parts[1]));
	//key = product id 	//value = "second <tab> product_name".. 
	//this is for reducer to understand that this record came from product table, marked by keyword "second". 
	
	
	}

}
