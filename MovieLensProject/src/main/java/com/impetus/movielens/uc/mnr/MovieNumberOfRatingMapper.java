package com.impetus.movielens.uc.mnr;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieNumberOfRatingMapper extends
		Mapper<LongWritable, Text, LongWritable, Text> {

	Text data = new Text();

	int movieId = Integer.MIN_VALUE;

	protected void map(
			LongWritable key,
			Text value,
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws java.io.IOException, InterruptedException {

		String[] fileSplit = value.toString().split("::");

		// movie rating file has 3 fields . Hence the length of the record in
		// the file would
		// also be 3
		if (fileSplit.length == 3) {
			movieId = Integer.valueOf(fileSplit[0]);
			data.set(fileSplit[1]);
//			System.out.println("File split = 3 --> movieId =" + movieId + "-data-  "+ data  );

		} else if (fileSplit.length == 4) {
			movieId = Integer.valueOf(fileSplit[1]);
			data.set(fileSplit[2]);
//			System.out.println("File split = 3 --> movieId =" + movieId + "-data-  "+ data  );

		}
		if (movieId > 0) {
			context.write(new LongWritable(movieId), data);
		} else {
			System.err.println("Invalid movie id " + movieId + "data : "
					+ data.toString());
		}

	};

	// not used... currently ..
	// TODO have plans of using this method for checking on a different logic
	// which would be implemented
	private boolean checkForNumber(String field) {

		Pattern p = Pattern.compile("[0-9].");
		Matcher m = p.matcher(field);

		return (m.find()) ? true : false;
	}

}
