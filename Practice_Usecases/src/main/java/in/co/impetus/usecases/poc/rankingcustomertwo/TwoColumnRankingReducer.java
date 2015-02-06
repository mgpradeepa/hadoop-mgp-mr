package in.co.impetus.usecases.poc.rankingcustomertwo;

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
public class TwoColumnRankingReducer extends
		Reducer<Text, NewCustomer, NullWritable, Text> {
	Log log = LogFactory.getLog(this.getClass());

	NullWritable n = NullWritable.get();

	/**
	 * optimize so that key in and value in need not be customer
	 */
	protected void reduce(Text cus, java.lang.Iterable<NewCustomer> custValues,
			Context cont) throws java.io.IOException, InterruptedException {
		int rank = 0;

		for (NewCustomer c : custValues) {

			rank = rank + 1;
			System.out.println(c.getCustomerId() + "::" + c.getScore() + " -> "
					+ c.getLocation() + " > " + rank);
			cont.write(n, new Text(c.getCustomerId() + "::" + c.getScore()
					+ " -> " + c.getLocation() + " > " + rank));
		}

	};

}
