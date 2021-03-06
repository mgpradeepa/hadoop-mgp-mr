package in.co.impetus.usecases.Practice_Usecases.mapper;

import in.co.impetus.usecases.Practice_Usecases.entity.Customer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OneColumnRankingMapper extends Mapper<LongWritable, Text, Customer, Customer>{//Mapper<LongWritable, Text, Text, Customer> {
	
	Customer custOut =null;
	
	Text keyOut = new Text();
	NullWritable  nul = NullWritable.get();
	
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		String[] lineRecord  = value.toString().split(",");
		
//		keyOut = new Text(lineRecord[0] + "_" +lineRecord[1]);
//		
//		custOut = new Customer(lineRecord[0], Long.parseLong(lineRecord[1]));
////		custOut.setCustId(lineRecord[0]);
////		custOut.setScore(Double.parseDouble(lineRecord[1]));
//		
//		context.write(keyOut, custOut);
		
		/**
		 * as we need to sort the table data depending on the basic fields its apt to have it in the key => as a composite key.
		 * As a composite key here the class called Customer has been created.
		 * 
		 *  For simplicity key and value both are assigned with the customer onject
		 */
		custOut = new Customer(lineRecord[0], Long.parseLong(lineRecord[1]));
		context.write(custOut, custOut);
		
		
		
		
	};
	
	
	

}
