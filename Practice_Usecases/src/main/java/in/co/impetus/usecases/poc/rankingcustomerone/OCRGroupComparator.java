package in.co.impetus.usecases.poc.rankingcustomerone;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OCRGroupComparator extends WritableComparator{
	protected OCRGroupComparator(){
		super(Customer.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
	Customer c1 = (Customer) a;
	Customer c2 = (Customer) b;
		
		return c1.getScore().compareTo(c2.getScore());
	}

}
