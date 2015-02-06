package in.co.impetus.sample.rsj.mapper;

import in.co.impetus.sample.rsj.entity.CompositeKeyWritable;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MapperRSJ extends
		Mapper<LongWritable, Text, CompositeKeyWritable, Text> {

	CompositeKeyWritable ckey = new CompositeKeyWritable();

	Text txt = new Text("");

	int sourceIndex = 0;

	StringBuilder sb = new StringBuilder();

	List<Integer> listOfReqAttribList = new ArrayList<Integer>();

	protected void setup(Context context) throws java.io.IOException,
			InterruptedException {
		// Get the source Index and then proceed
		FileSplit fsFileSplit = (FileSplit) context.getInputSplit();
		System.out.println(fsFileSplit.toString());
		sourceIndex = Integer.parseInt(context.getConfiguration().get(
				fsFileSplit.getPath().getName()));

		if (sourceIndex == 1) {// for employeee
			listOfReqAttribList.add(2); // Name
			listOfReqAttribList.add(3); // joining date
			listOfReqAttribList.add(4); // dept no.
			listOfReqAttribList.add(6);

		} else if(sourceIndex!=4){ // for salary

			listOfReqAttribList.add(1); // Salary
			listOfReqAttribList.add(3); // Effective-to-date (Value of
			// 9999-01-01 indicates current
			// salary)

		}

	};

	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		if (value.toString().length() > 0) {
			String arrEntityAttrib[] = value.toString().split(",");
			ckey.setJoinKey(arrEntityAttrib[0].toString());
			ckey.setSourceIndex(sourceIndex);

			txt.set(buildMapValue(arrEntityAttrib));
			context.write(ckey, txt);
		}

	}

	private String buildMapValue(String[] arrEntityAttrib) {
		sb.setLength(0);
		// Build list of attributes to output based on Employee or Salary

		for (int i = 0; i < arrEntityAttrib.length; i++) {
			// If the field is in the list of required output append to
			// stringbuilder
			if (listOfReqAttribList.contains(i)) {
				sb.append(arrEntityAttrib[i]).append(",");
			}
		}
		if (sb.length() > 0) {
			// Drop last comma
			sb.setLength(sb.length() - 1);
		}

		return sb.toString();
	};

}
