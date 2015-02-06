package in.co.impetus.usecases.poc.rankingcustomertwo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TwoRGroupComparator extends WritableComparator {
	protected TwoRGroupComparator() {
		super(NewCustomer.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		NewCustomer c1 = (NewCustomer) a;
		NewCustomer c2 = (NewCustomer) b;
		int	result = c1.getCustomerId().compareTo(c2.getCustomerId());
		if(result ==0) {			
			result = c1.getLocation().compareTo(c2.getLocation());		
			if (result == 0) {
				result = -1 * (c1.getScore().compareTo(c2.getScore()));
			}
		
		}
		return result;
	}

}
