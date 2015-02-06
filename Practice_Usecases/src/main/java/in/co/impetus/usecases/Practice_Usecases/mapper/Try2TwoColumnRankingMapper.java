package in.co.impetus.usecases.Practice_Usecases.mapper;

import in.co.impetus.usecases.Practice_Usecases.entity.NewCustomer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Try2TwoColumnRankingMapper extends Mapper<LongWritable, Text, Text, NewCustomer>{//Mapper<LongWritable, Text, Text, Customer> {
	
	NewCustomer custOut =null;
	
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
		keyOut = new Text(lineRecord[0] + "_" + lineRecord[2]);
		custOut = new NewCustomer(lineRecord[0], lineRecord[1], lineRecord[2]);
		context.write(keyOut, custOut);
	}
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,NewCustomer>.Context context) throws java.io.IOException ,InterruptedException {
		context.getConfiguration().clear();
		
		
	};
}
