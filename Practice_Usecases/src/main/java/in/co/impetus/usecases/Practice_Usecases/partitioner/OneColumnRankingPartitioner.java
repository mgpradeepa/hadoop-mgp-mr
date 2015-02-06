package in.co.impetus.usecases.Practice_Usecases.partitioner;

import in.co.impetus.usecases.Practice_Usecases.entity.Customer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 
 * @author pradeep
 * 
 *         Just to have all the customer with respective id get partitioned to
 *         one reducer for efficiency .
 */
public class OneColumnRankingPartitioner extends
		Partitioner<Customer, Customer> {
	@Override
	public int getPartition(Customer cust, Customer arg1, int numPart) {
		/**
		 * custid is used for partitioning.
		 */
		return cust.getCustId().hashCode() % numPart;

	}

}
