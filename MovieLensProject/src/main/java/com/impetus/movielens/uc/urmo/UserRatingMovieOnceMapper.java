package com.impetus.movielens.uc.urmo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserRatingMovieOnceMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {

	final String splitter = "::" ;
	Text userIdKey = new Text();
	NullWritable nulValue = NullWritable.get();
	protected void map(
			LongWritable key,
			Text value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		
		String[] lineRecord = value.toString().split(splitter);
		userIdKey.set(lineRecord[0]);
		
		
		context.write(userIdKey, nulValue);
		
		
	};

}
