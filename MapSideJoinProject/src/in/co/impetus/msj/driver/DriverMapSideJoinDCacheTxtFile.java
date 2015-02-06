package in.co.impetus.msj.driver;

import in.co.impetus.msj.app.MapperMapSideJoinDCacheTextFile;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverMapSideJoinDCacheTxtFile extends Configured implements Tool {
	public static URI[] cacheLocalFiles;

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out
					.printf("Two parameters are required- <input dir> <output dir>\n");
			return -1;
		}

		Job job = new Job(getConf());

		// Configuration configuration = new Configuration();
		job.setJobName("Map-side join with text lookup file in DCache");

		// The path of the local file system where the file contender for Distributed Cache is reciding.
		URI uri = new URI(
				"/home/ubuntu/Public/MGP/DataFolder/Empl_Dept/dept.txt");
		
		// resolving of URI not required. Its just a validation process
		URI resolve = uri
				.resolve("/home/ubuntu/Public/MGP/DataFolder/Empl_Dept/dept.txt");
		
		DistributedCache.addCacheFile(uri, getConf());// - >
														// hdfs://localhost:54310/user/ubuntu/mgp/JoinProject/input/Empl_Dept/dept.txt"
		cacheLocalFiles = DistributedCache.getCacheFiles(getConf());
		job.setJarByClass(DriverMapSideJoinDCacheTxtFile.class);
		FileInputFormat
				.addInputPaths(job,
						"hdfs://localhost:54310/user/ubuntu/mgp/JoinProject/input/Empl_Dept/");
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(MapperMapSideJoinDCacheTextFile.class);
		job.setNumReduceTasks(0);

		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),
				new DriverMapSideJoinDCacheTxtFile(), args);
		System.exit(exitCode);
	}
}
