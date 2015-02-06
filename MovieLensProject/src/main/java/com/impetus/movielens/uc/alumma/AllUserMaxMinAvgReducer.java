package com.impetus.movielens.uc.alumma;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AllUserMaxMinAvgReducer extends Reducer<Text, Text, Text, Text> {


	String interimData = null;
	String[] splittableString = null;

	// userid :: movieid::rating::timestamp
	protected void reduce(
			Text key,
			java.lang.Iterable<Text> values,
			org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {
		double max = 0;
		double min = 6;
		double totalEntries = 0L;
		int count = 0;
		double temp = 0.0;
		for (Text text : values) {

			interimData = new String(text.toString());
			// movieid_rating_timestamp
			splittableString = interimData.split("_");
			temp = Double.parseDouble(splittableString[1]); // 4.5,5,2,3,3.5,1.9
			totalEntries += temp;

			if (temp > max) {
				max = temp;  // max = 4.5, 5

			}

			if (temp < min) {
				min = temp; // min = 4.5, 2
			}
			count++;
		}
		System.out.println("KEY = " + key.toString() + " MAX = " + max
				+ " MIN = " + min + " AVERAGE= " + (totalEntries / (double)count));

		context.write(key, new Text("MAX = " + max + " MIN = " + min
				+ " AVERAGE= " + (totalEntries / (double)count)));
	};

}
