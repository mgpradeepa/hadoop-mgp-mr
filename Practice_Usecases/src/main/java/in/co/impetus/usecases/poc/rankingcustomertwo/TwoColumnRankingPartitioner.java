package in.co.impetus.usecases.poc.rankingcustomertwo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 
 * @author pradeep
 * 
 *         Just to have all the customer with respective id get partitioned to
 *         one reducer for efficiency .
 */
public class TwoColumnRankingPartitioner extends Partitioner<Text, NewCustomer> {
	@Override
	public int getPartition(Text key, NewCustomer arg1, int numPart) {
		/**
		 * customerId is used for partitioning.
		 */

		String[] splitter = key.toString().split("_");
		Integer firstItem = new Integer(splitter[0]);
		System.out.println("FIRST ITEM  -> " + firstItem);
		return (firstItem * 173) % numPart;

	}
}
