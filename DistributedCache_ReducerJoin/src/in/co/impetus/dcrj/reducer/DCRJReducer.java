package in.co.impetus.dcrj.reducer;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DCRJReducer extends Reducer<LongWritable, Text, Text, Text> {
	private Set<String> aset = new HashSet<String>();
	private Set<String> cset = new HashSet<String>();

	private Text finalKey = new Text();
	private Text finalValue = new Text();

	protected void reduce(LongWritable key, java.lang.Iterable<Text> values,
			Context context) throws java.io.IOException, InterruptedException {

//		aset.clear();
//		bset.clear();

		for (Text val : values) {
			if (val != null) {
				if (val.toString().startsWith("A:")) {
					aset.add(val.toString().substring(2));
				}
				if (val.toString().startsWith("C:")) {
					cset.add(val.toString().substring(2));
				}
			}
		}

		for (String astr : aset) {
			System.out.println("ASTR -> " + astr);
			for (String bStr : cset) {
				System.out.println("BSTR -> " + bStr);
				finalKey.clear();
				finalValue.clear();
				finalKey.set(astr);
				finalValue.set(bStr);
				System.out.println("finalKey -> " + finalKey + " :: finalKey"
						+ finalValue);
				context.write(finalKey, finalValue);

			}
		}

	};

}
