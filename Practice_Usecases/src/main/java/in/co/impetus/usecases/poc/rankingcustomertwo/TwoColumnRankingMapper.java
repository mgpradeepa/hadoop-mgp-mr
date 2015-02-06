package in.co.impetus.usecases.poc.rankingcustomertwo;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TwoColumnRankingMapper extends Mapper<LongWritable, Text, Text, NewCustomer>{//Mapper<LongWritable, Text, Text, Customer> {
	
	NewCustomer custOut =null;
	
	Text keyOut = new Text();
	NullWritable  nul = NullWritable.get();
	
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		String[] lineRecord  = value.toString().split(",");
		
		
		/**
		 * key is formed in such a way that the required comparing field are associated
		 * value is the customer itelf 
		 */
		keyOut = new Text(lineRecord[0] + "_" + lineRecord[2]);
		custOut = new NewCustomer(lineRecord[0], lineRecord[1], lineRecord[2]);
		context.write(keyOut, custOut);
	}

}
