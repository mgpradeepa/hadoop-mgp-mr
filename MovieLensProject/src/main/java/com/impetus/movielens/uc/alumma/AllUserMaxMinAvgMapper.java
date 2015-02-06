package com.impetus.movielens.uc.alumma;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AllUserMaxMinAvgMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	final String splitter = "::";
	public final String separator = "_";
	Text userIdKey = new Text();
	Text ratingData = new Text();
	String uidMid = null;

	protected void map(LongWritable key, Text values, Context context)
			throws java.io.IOException, InterruptedException {

		// userid :: movieid::rating::timestamp
		String[] lineRecord = values.toString().split(splitter);
		userIdKey.set(lineRecord[0]);
		ratingData.set(lineRecord[1] + separator + lineRecord[2] + separator
				+ lineRecord[3]);

		context.write(userIdKey, ratingData);

	};
}
