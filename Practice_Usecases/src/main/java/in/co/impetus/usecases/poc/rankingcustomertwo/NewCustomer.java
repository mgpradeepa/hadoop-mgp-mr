package in.co.impetus.usecases.poc.rankingcustomertwo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class NewCustomer implements WritableComparable<NewCustomer> {

	private String customerId;
	private String score;
	private String location;

	public NewCustomer() {
		// TODO Auto-generated constructor stub
	}

	public NewCustomer(String customerId, String score, String location) {
		super();
		this.customerId = customerId;
		this.score = score;
		this.location = location;
		
	}
	

	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public String getScore() {
		return score;
	}

	public void setScore(String score) {
		this.score = score;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public void readFields(DataInput in) throws IOException {

		customerId = WritableUtils.readString(in);
		score = WritableUtils.readString(in);
		location = WritableUtils.readString(in);

	}

	public void write(DataOutput out) throws IOException {

		WritableUtils.writeString(out, customerId);
		WritableUtils.writeString(out, score);
		WritableUtils.writeString(out, location);

	}

	public int compareTo(NewCustomer nc) {

		int result = customerId.compareTo(nc.customerId);
		if (result ==0) {
			result = location.compareTo(nc.location);
			if (result == 0) {
				result =-1*(score.compareTo(nc.score));
			}
		}

		return result;

	}



}
