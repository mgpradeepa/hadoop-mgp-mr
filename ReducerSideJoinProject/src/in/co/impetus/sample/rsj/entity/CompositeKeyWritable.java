package in.co.impetus.sample.rsj.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {

	// Data memebers
	private String joinKey; // joinKey -> employeeID
	private int sourceIndex; // 1=Employee data; 2=Salary (current) data;
								// 3=Salary historical data

	public CompositeKeyWritable() {
		// nothing required
	}

	public CompositeKeyWritable(String joinKey, int sourceIndex) {
		super();
		this.joinKey = joinKey;
		this.sourceIndex = sourceIndex;
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(joinKey).append("\t")
				.append(sourceIndex)).toString();

	}

	@Override
	public void readFields(DataInput input) throws IOException {
		joinKey = WritableUtils.readString(input);
		sourceIndex = WritableUtils.readVInt(input);

	}

	@Override
	public void write(DataOutput output) throws IOException {
		WritableUtils.writeString(output, joinKey);

		WritableUtils.writeVInt(output, sourceIndex);

	}

	@Override
	public int compareTo(CompositeKeyWritable obj) {

		int result = joinKey.compareTo(obj.joinKey);
		if (0 == result) {
			result = Double.compare(sourceIndex, obj.sourceIndex);
		}

		return result;
	}

	public String getJoinKey() {
		return joinKey;
	}

	public void setJoinKey(String joinKey) {
		this.joinKey = joinKey;
	}

	public int getSourceIndex() {
		return sourceIndex;
	}

	public void setSourceIndex(int sourceIndex) {
		this.sourceIndex = sourceIndex;
	}

}
