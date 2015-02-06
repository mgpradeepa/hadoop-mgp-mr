package in.co.impetus.sample.rsj.util;

import in.co.impetus.sample.rsj.entity.CompositeKeyWritable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparatorRSJ extends WritableComparator {
	protected GroupingComparatorRSJ() {
		super(CompositeKeyWritable.class, true);

	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CompositeKeyWritable k1 = (CompositeKeyWritable) a;
		CompositeKeyWritable k2 = (CompositeKeyWritable) b;

		return (k1.getJoinKey().compareTo(k2.getJoinKey()));
	}
}
