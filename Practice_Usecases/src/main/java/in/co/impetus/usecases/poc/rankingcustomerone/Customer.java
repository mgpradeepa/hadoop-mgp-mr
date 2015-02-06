package in.co.impetus.usecases.poc.rankingcustomerone;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class Customer implements WritableComparable<Customer> {

	public Customer() {

	}

	private String custId;
	private Long score;

	public String getCustId() {
		return custId;
	}

	public void setCustId(String custId) {
		this.custId = custId;
	}

	public Long getScore() {
		return score;
	}

	public void setScore(Long score) {
		this.score = score;
	}

	public Customer(String custId, Long score) {
		super();
		this.custId = custId;
		this.score = score;
	}

	public void readFields(DataInput in) throws IOException {

		custId = WritableUtils.readString(in);
		score = WritableUtils.readVLong(in);
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, custId);
		WritableUtils.writeVLong(out, Long.parseLong(score.toString()));

	}

	public int compareTo(Customer c) {
		int result = score.compareTo(c.score);
		if (0 == result) {
			result = (Double.compare(score, c.score));
		}
		
		return result; // may have to change to - "minus " for descending order

	}

	@Override
	public String toString() {
		return  custId + " ::" + score ;
	}
	

}
