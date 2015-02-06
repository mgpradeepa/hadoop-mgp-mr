package in.co.impetus.usecases.Practice_Usecases.reducer;

import in.co.impetus.usecases.Practice_Usecases.entity.NewCustomer;

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
public class Try2TwoColumnRankingReducer extends
		Reducer<Text, NewCustomer, NullWritable, Text> {
	// Reducer<Text, Customer, NullWritable, Text> {

	// static int rank = 0;
	Log log = LogFactory.getLog(this.getClass());

	NullWritable n = NullWritable.get();

	/**
	 * optimize so that key in and value in need not be customer
	 */
	protected void reduce(Text cus, java.lang.Iterable<NewCustomer> custValues,
			Context cont) throws java.io.IOException, InterruptedException {
		int rank = 0;
		// int tempRank =0;
		//
		// String tempCustId;
		// Iterator< NewCustomer> custIter = custValues.iterator();

		// NewCustomer nc1 = null;
		// NewCustomer nc2 = null;
		//
		// while(custIter.hasNext()) {
		// tempRank++;
		// nc1 = custIter.next();
		//
		// if(null!= custIter.next()){
		// nc2 = custIter.next();
		// if((nc1.getLocation().equalsIgnoreCase(nc2.getLocation())) &&
		// (nc1.getCustomerId().equalsIgnoreCase (nc2.getCustomerId()))){
		// nc1.setRank(rank++);
		// nc2.setRank(rank++);
		//
		//
		// }
		//
		// }else{
		//
		// nc1.setRank(tempRank);
		// }
		// log.info(nc1.getCustomerId() + "::" + nc1.getScore() + " -> "
		// + nc1.getLocation() + ":: " + nc1.getRank());
		// if(nc1 != null) {
		// cont.write(n, new Text(nc1.getCustomerId() + "::" + nc1.getScore() +
		// " -> "
		// + nc1.getLocation() + ":: " + nc1.getRank()));
		// }else {
		//
		// cont.write(n, new Text(""));
		// }
		// }

		for (NewCustomer c : custValues) {

			// tempCustId = c.getCustomerId();
			// if(tempCustId.equalsIgnoreCase(cus.getCustomerId())) {
			// if(c.getLocation().equalsIgnoreCase(c.getLocation()))
			// }
			// log.info(tempCustId);

			// while(tempCustId == c.getCustomerId()) {
			rank = rank + 1;
			// rank++;
			// log.info(c.getCustomerId() + "::" + c.getScore() + " -> "
			// + c.getLocation() + ":: " + ++rank);
			System.out.println(c.getCustomerId() + "::" + c.getScore() + " -> "
					+ c.getLocation() + " > " + rank);
			cont.write(n, new Text(c.getCustomerId() + "::" + c.getScore()
					+ " -> " + c.getLocation() + " > " + rank));
		}
		// if(c.getCustId().equals(anObject))

		// }
		// rank = 0;

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
