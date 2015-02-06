package co.in.impetus.hadoop.mr.examples.ml.driver;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import co.in.impetus.hadoop.filesystem.FileUtility;
import co.in.impetus.hadoop.mr.examples.ml.mappers.MovieNRMapper;
import co.in.impetus.hadoop.mr.examples.ml.reducers.MovieNRReducer;

public class MovieNumberOfRating  extends BaseJobInitiator{
	public static final String PROPERTY_FILE = "usecase.properties";
	public static final String INPUT_DIRECTORY = "inputDirectory";
	public static final String OUTPUT_DIRECTORY = "outputDirectory";

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();
		conf.setFloat("io.sort.spill.percent", (float) 0.80);
		System.out.println("Printing all the configuration details for the Current project");
		System.out.println("**************************************************\n **********************************\n");
		for(Entry<String, String> entry:conf) {
			System.out.printf("%s = %s\n", entry.getKey(), entry.getValue());

		}
		System.out.println("\n\n**************************************************\n **********************************\n\n");
		
		Job job = new Job();
		job.setJarByClass(co.in.impetus.hadoop.mr.examples.ml.driver.MovieNumberOfRating.class);

		Properties prop = new Properties();
		InputStream input = null;
		input = MovieNumberOfRating.class.getClassLoader().getResourceAsStream(
				PROPERTY_FILE);// new FileInputStream(PROPERTY_FILE);
		prop.load(input);

		// deleting the files from HDFS
		FileSystem fs = FileSystem.get(conf);
		Path outputDIrectory = new Path(prop.getProperty(OUTPUT_DIRECTORY));
		fs.delete(outputDIrectory, true);
		System.out
				.println("Now hdfs output directory should have been deleted");

		// specify input and output dirs
		FileInputFormat.addInputPath(job,
				new Path(prop.getProperty(INPUT_DIRECTORY)));

		File f = new File(outputDIrectory.toString());

		// utility class to delete the files
		FileUtility.deleteOPFiles(f);

		FileOutputFormat.setOutputPath(job, outputDIrectory);
		System.out.println(outputDIrectory.toString());

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);

		// specify ouput types
		// job.setMapOutputKeyClass(LongWritable.class);
		// job.setMapOutputValueClass(LongWritable.class);

		// set Mappers and Reducers
		job.setMapperClass(MovieNRMapper.class);
		job.setCombinerClass(MovieNRReducer.class);
		job.setReducerClass(MovieNRReducer.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
	
	

}
