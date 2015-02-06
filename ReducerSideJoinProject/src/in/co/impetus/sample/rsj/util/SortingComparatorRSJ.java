package in.co.impetus.sample.rsj.util;

import in.co.impetus.sample.rsj.entity.CompositeKeyWritable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortingComparatorRSJ extends WritableComparator{
	
	protected SortingComparatorRSJ(){
		super(CompositeKeyWritable.class, true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {

		
		CompositeKeyWritable key1 = (CompositeKeyWritable) a;
		CompositeKeyWritable key2 = (CompositeKeyWritable) b;
		
		
		int compResult = key1.getJoinKey().compareTo(key2.getJoinKey());
		if(compResult == 0) // tells whether the same join key is encountered
		{
			return Double.compare(key1.getSourceIndex(), key2.getSourceIndex());
		}
		
		
		return compResult;
	}

}
