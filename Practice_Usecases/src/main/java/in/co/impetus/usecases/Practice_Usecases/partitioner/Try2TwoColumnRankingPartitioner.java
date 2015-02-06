package in.co.impetus.usecases.Practice_Usecases.partitioner;

import in.co.impetus.usecases.Practice_Usecases.entity.NewCustomer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 
 * @author pradeep
 * 
 *         Just to have all the customer with respective id get partitioned to
 *         one reducer for efficiency .
 */
public class Try2TwoColumnRankingPartitioner extends
		Partitioner<Text, NewCustomer> {
	@Override
	public int getPartition(Text key, NewCustomer arg1, int numPart) {
		/**
		 * customerId is used for partitioning.
		 */
		// int resultPartition = ((cust.getCustomerId().hashCode() * 17) +
		// (cust.getLocation().hashCode() * 11 ))% numPart;;

		String[] splitter = key.toString().split("_");
Integer one = new Integer(splitter[0]);
System.out.println("ONE -> " + one);
//Integer two= new Integer(splitter[1].hashCode());
//System.out.println("TWO -> " + two);

		int party = (one * 173)
				 % numPart;
//		int resultPartition = (new Integer(cust.getCustomerId()).intValue() + cust
//				.getLocation().toLowerCase().hashCode())
//				% numPart;
//		System.out.println(cust.getCustomerId() + cust.getLocation() + numPart
//				+ "#" + resultPartition);
		System.out.println(party + " partitions");

		// return cust.getCustomerId().hashCode() % numPart;
		return party;

	}
}
