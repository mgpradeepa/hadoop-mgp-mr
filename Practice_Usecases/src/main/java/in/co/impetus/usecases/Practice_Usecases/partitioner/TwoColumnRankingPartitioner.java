package in.co.impetus.usecases.Practice_Usecases.partitioner;

import in.co.impetus.usecases.Practice_Usecases.entity.NewCustomer;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 
 * @author pradeep
 * 
 *         Just to have all the customer with respective id get partitioned to
 *         one reducer for efficiency .
 */
public class TwoColumnRankingPartitioner extends
		Partitioner<NewCustomer, NewCustomer> {
	@Override
	public int getPartition(NewCustomer cust, NewCustomer arg1, int numPart) {
		/**
		 * customerId is used for partitioning.
		 */
		// int resultPartition = ((cust.getCustomerId().hashCode() * 17) +
		// (cust.getLocation().hashCode() * 11 ))% numPart;;
		int resultPartition = (new Integer(cust.getCustomerId()).intValue() + cust
				.getLocation().toLowerCase().hashCode())
				% numPart;
		System.out.println(cust.getCustomerId() + cust.getLocation() + numPart
				+ "#" + resultPartition);

		// return cust.getCustomerId().hashCode() % numPart;
		return resultPartition;

	}
}
