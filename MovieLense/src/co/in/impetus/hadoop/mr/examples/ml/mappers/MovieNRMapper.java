package co.in.impetus.hadoop.mr.examples.ml.mappers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieNRMapper extends
		Mapper<LongWritable, Text, LongWritable, LongWritable> {

	public static final String DOUBLE_COLON = "::";
	public static final String SPACE_SEPARATOR = "\\s+";
	
	public static final LongWritable ONE = new LongWritable(1);

	Text movieItem = new Text();

	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
			
			String[] movies = value.toString().split(DOUBLE_COLON);
			movieItem.set(movies[1]);
			context.write(new LongWritable(new Long(movies[1])), ONE);
			
		

	}
/**
 * public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
		
		String recordValue = value.toString();
		String[] columnFields = recordValue.split(fieldSeparator);
		
		LOGGER.info("columnFields[0]::: movieId: " + columnFields[0]);
		LOGGER.info("columnFields[1]::: userId: " + columnFields[1]);
		
		String movieId = columnFields[0];
		int userId = Integer.parseInt(columnFields[1]);
		
		String movieName = MovieRatingLoader.getProperty(movieId);
		LOGGER.info("movieName:: " + movieName);
		
		boolean isValidRegex = applyRegexCheck(expectedRegex, movieName);
		
		LOGGER.info("isValidRegex:: " + isValidRegex);
		
		if(isValidRegex){
			context.write(new Text(movieName), new IntWritable(userId));
		}
		
 */
}
