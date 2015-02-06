package in.co.impetus.usecases.poc.rankingcustomerone;



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
