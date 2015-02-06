package in.co.impetus.usecases.poc.rankingcustomerone;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OCRSortComparator extends WritableComparator {
	protected OCRSortComparator() {
		super(Customer.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Customer c1 = (Customer) a;
		Customer c2 = (Customer) b;
		int result = 0;
		/**
		 * First start sorting on the customer id and and correspondingly sort
		 * the score in descending order. -1 * (x) => for descending order
		 */
		result = c1.getCustId().compareTo(c2.getCustId());
		if (result == 0) {
			return -1 * (c1.getScore().compareTo(c2.getScore()));
		}
		return result;
	}
}
