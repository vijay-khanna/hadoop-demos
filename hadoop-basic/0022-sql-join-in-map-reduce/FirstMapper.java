package join;

/*
 * for every record in the tables, key = join-attribute, value = remaining attributes in table. 
*   File 1 = transactions.csv. 	: User_ID, Product_ID
*   File 2 = products.csv. 	  	: Product_ID, Product_Name

*   List the user ID, and product name which they purchased using Join,
* 
* This FirstMapper class handles the Transaction Table/csv
* 0th col = user id.
* 1st col = Product ID

* in transactions record write function : //key = product_id .value = "TransactionRecord_FirstTable <tab> user_id"
* 
* 
* in products record write function 	: //key = product_id .Value = "ProductRecord_SecondTable <tab> product_name" 

 */
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

import org.apache.hadoop.io.*;

public class FirstMapper extends Mapper<LongWritable,Text,Text,Text>{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{String record = value.toString();
	String[] parts = record.split(",");
	context.write(new Text(parts[1]), new Text("TransactionRecord_FirstTable\t" + parts[0]));
	//key = product_id, value = "first <tab> user_id"
	
	
		
		
	
	}
}
