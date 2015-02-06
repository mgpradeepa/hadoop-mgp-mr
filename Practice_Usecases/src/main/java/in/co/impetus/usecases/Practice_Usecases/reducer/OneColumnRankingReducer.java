package in.co.impetus.usecases.Practice_Usecases.reducer;

import in.co.impetus.usecases.Practice_Usecases.entity.Customer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author ubuntu
 * 
 */
public class OneColumnRankingReducer extends
		Reducer<Customer, Customer, NullWritable, Text> {
	// Reducer<Text, Customer, NullWritable, Text> {
	
	Log log = LogFactory.getLog(this.getClass());

	NullWritable n = NullWritable.get();

	/**
	 * optimize so that key in and  value in need not be customer
	 */
	protected void reduce(Customer cus,
			java.lang.Iterable<Customer> custValues, Context cont)
			throws java.io.IOException, InterruptedException {
		int rank = 0;
		String tempCustId;
		for (Customer c : custValues) {
			tempCustId = c.getCustId();
			log.info(tempCustId);
			
			while(tempCustId == c.getCustId()) {
				
				rank++;				
				log.info(c.getCustId() + "::" + c.getScore() + " -> " + rank);
				cont.write(n, new Text(c.getCustId() + "::" + c.getScore() + " -> " + rank) );
			}
//			if(c.getCustId().equals(anObject))

		}

	};

	/**
	 * can be done in the below way as well. But result are not assured.
	 */
	// protected void reduce(Text key, java.lang.Iterable<Customer> values,
	// Context context) throws java.io.IOException, InterruptedException {
	//
	// // Iterator<Customer> iterValues = values.iterator();
	//
	// for(Customer c : values) {
	// valueout = new Text(c.getCustId() + " :: "+ c.getScore().toString());
	// context.write(n, valueout);
	// }
	// // while(iterValues.hasNext()){
	// // Customer c = iterValues.next();
	// //// keyout = new Text(c.getCustId());
	// // valueout = new Text(c.getCustId() + " :: "+ c.getScore().toString());
	// // }
	//
	// };

}
