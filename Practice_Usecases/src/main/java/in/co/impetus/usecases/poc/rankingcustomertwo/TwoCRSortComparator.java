package in.co.impetus.usecases.poc.rankingcustomertwo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TwoCRSortComparator extends WritableComparator {
	protected TwoCRSortComparator() {
		super(NewCustomer.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		NewCustomer c1 = (NewCustomer) a;
		NewCustomer c2 = (NewCustomer) b;
		int result = 0;
		/**
		 * First start sorting on the customer id and and correspondingly sort
		 * the score in descending order. -1 * (x) => for descending order
		 */
		result = c1.getCustomerId().compareTo(c2.getCustomerId());
		if (result == 0) {
			result = c1.getLocation().compareToIgnoreCase(c2.getLocation());
			if (result == 0) {
				return -1 * (c1.getScore().compareTo(c2.getScore()));
			}
		}
		return result;
	}
}
