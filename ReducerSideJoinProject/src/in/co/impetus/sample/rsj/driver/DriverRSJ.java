package in.co.impetus.sample.rsj.driver;

import in.co.impetus.sample.rsj.entity.CompositeKeyWritable;
import in.co.impetus.sample.rsj.entity.ReducersideJoinRSJ;
import in.co.impetus.sample.rsj.mapper.MapperRSJ;
import in.co.impetus.sample.rsj.partitioner.RSJPartitioner;
import in.co.impetus.sample.rsj.util.GroupingComparatorRSJ;
import in.co.impetus.sample.rsj.util.SortingComparatorRSJ;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverRSJ extends Configured implements Tool {
	public static URI[] cacheLocalFiles;
	public static List<URI> uriAsDC = new ArrayList<URI>();

	@Override
	public int run(String[] args) throws Exception {
		// {{
		// Exit job if required arguments have not been provided
		if (args.length != 2) {
			System.out
					.printf("Two parameters are required for DriverRSJ- <input dir1>  <output dir>\n");
			return -1;
		}
		// }{

		// {{
		// Job instantiation
		Job job = new Job(getConf());
		Configuration conf = job.getConfiguration();
		job.setJarByClass(DriverRSJ.class);
		job.setJobName("ReduceSideJoin");
		// }}

		// {{
		// Add side data to distributed cache
		URI uri = new URI(
				"/user/ubuntu/mgp/JoinProject/input/Empl_Dept/withsalData/REdJoin_Df/departments_map.txt");
				//"/home/ubuntu/Public/MGP/DataFolder/Empl_Dept/dept.txt");
		URI resolve = uri.resolve(uri);
//		uriAsDC.add(uri);
		/**
		 * Temporarily moved from dstributed Cache to List
		 */
		 DistributedCache.addCacheFile(uri, getConf());
		 cacheLocalFiles = DistributedCache.getCacheFiles(getConf());
		/**
		 * Temporarily moved from dstributed Cache to List
		 */
		// }}

		// {
		// Set sourceIndex for input files;
		// sourceIndex is an attribute of the compositeKey,
		// to drive order, and reference source
		// Can be done dynamically; Hard-coded file names for simplicity
		// conf.setInt("part-e", 1);// Set Employee file to 1
		//
		// conf.setInt("part-sc", 2);// Set Current salary file to 2
		// conf.setInt("part-sh", 3);// Set Historical salary file to 3

		conf.setInt("emp.txt", 1);// Set Employee file to 1

		conf.setInt("sal_cur.txt", 2);// Set Current salary file to 2
		conf.setInt("sal_hist.txt", 3);// Set Historical salary file to 3
		conf.setInt("departments_map.txt",4); // Set Cache file to 4

		// }

		// {
		// Build csv list of input files
		StringBuilder inputPaths = new StringBuilder();
		inputPaths.append(args[0].toString());
		// }

		// {{
		// Configure remaining aspects of the job
		FileInputFormat.setInputPaths(job, inputPaths.toString());
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MapperRSJ.class);
		job.setMapOutputKeyClass(CompositeKeyWritable.class);
		job.setMapOutputValueClass(Text.class);

//		job.setPartitionerClass(RSJPartitioner.class);
		job.setSortComparatorClass(SortingComparatorRSJ.class);
		job.setGroupingComparatorClass(GroupingComparatorRSJ.class);

		job.setNumReduceTasks(4);
		job.setReducerClass(ReducersideJoinRSJ.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		// }}

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new Configuration(), new DriverRSJ(),
				args);
		System.exit(exitCode);

	}

}
