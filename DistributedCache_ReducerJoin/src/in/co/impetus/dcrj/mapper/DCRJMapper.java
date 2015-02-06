package in.co.impetus.dcrj.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class DCRJMapper  extends Mapper<LongWritable, Text, LongWritable, Text>{

	static{
		System.out.println("Mapper static cheker");
	}
	Text outVal = new Text();
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		System.out.println("Check1" +  value.toString());
		String[] lineSplit = value.toString().split("\\t");
		System.out.println("Check2" + lineSplit);
		FileSplit fs = (FileSplit) context.getInputSplit();
		System.out.println("Check3");
		String path = fs.getPath().toString();
		
		System.out.println("Check4");
		if(path.contains("attributes")) {
			System.out.println("Check5");
			outVal.set("A:" + lineSplit[0]);
			context.write(new LongWritable(Long.parseLong(lineSplit[1])), outVal);
			
			
		}else if(path.contains("customer")) {
			System.out.println("Check6");
			outVal.set("C:"+ lineSplit[0]);
			context.write(new LongWritable(Long.parseLong(lineSplit[1])), outVal);
		}
		
		
	};

}
