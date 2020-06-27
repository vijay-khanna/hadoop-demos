package join;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;

public class MrjoinReducer extends Reducer<Text,Text,Text,Text> {
	public void reduce(Text key, Iterable<Text> all_recods_TransactionsAndProducts_from_mapper_values, Context context)
			throws IOException, InterruptedException 
			{
			List<String>users_array = new ArrayList<String>();
			List<String>products_array = new ArrayList<String>();
			
			
			for (Text individual_Record_TorP : all_recods_TransactionsAndProducts_from_mapper_values) 
				// iterate through all the records, This individual record could be Transaction or for Product.  
				//for product id 101, get list of all user Id's who purchased this product id. 
				
				// * * * * * For Each product key, e.g. 101, we will get all user_id's who correspond to this product_id. 
				
			{ 
			String parts[] = individual_Record_TorP.toString().split("\t"); // split the value portion based on tab.
			if (parts[0].equals("TransactionRecord_FirstTable")) 
				{
				users_array.add(parts[1]);
				// parts[0] = TransactionRecord_FirstTable		// since this record comes from first mapper
				// parts[1] = user_id
				// transactions record write function : //key = product_id .value = "TransactionRecord_FirstTable <tab> user_id"
				// keep in mind the parts array is pointing to value portion only.  
				// the key is the product_id, send from the first-mapper. 
				
				
			//this record came from "first mapper", and it is transaction record.
			// add to users array. 
			} 
			else if (parts[0].equals("ProductRecord_SecondTable")) 
			{
				products_array.add(parts[1]);
			//this record came from "second mapper", and it is product record.
			// add product_name to products array. 
			// from second mapper : products_record : key = product_id .Value = "ProductRecord_SecondTable <tab> product_name"
			// 	parts[0] =  ProductRecord_SecondTable
			// 	parts[1] =  product_name
				
			}
			}
			for(String individual_user:users_array) {
				for(String individual_product_name:products_array) {
										
					String productID_and_ProductName_str = key+"\t"+individual_product_name; // key = join key = product_id here
					context.write(new Text(individual_user), new Text(productID_and_ProductName_str));
					//here key = user_id, and value is "product_id <tab> product_name"
					//will list 3 columns as output : user_id  product_id  product_name
					
				}
			}
			
			}

}
