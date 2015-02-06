package com.impetus.movielens.uc.unrfm;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserNumberOfRatingForMovieMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	final String splitter = "::" ;
	final String separator = "_" ;
	Text userId_movieId_key = new Text();
	IntWritable ONE = new IntWritable(1);
	String uidMid = null;
	protected void map(
			LongWritable key,
			Text value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws java.io.IOException, InterruptedException {
		
		String[] lineRecord = value.toString().split(splitter);
		uidMid = lineRecord[0]+separator+ lineRecord[1];
		
		userId_movieId_key.set(uidMid);
		context.write(userId_movieId_key, ONE);
		
		
	};

}
