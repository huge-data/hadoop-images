package zx.soft.hdfs.images.edgedetect;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;

public class LongArrayWritable extends ArrayWritable {

	public LongArrayWritable() {
		super(LongWritable.class);
	}

	public LongArrayWritable(LongWritable[] values) {
		super(LongWritable.class, values);
	}

	@Override
	public String toString() {
		String[] strings = toStrings();
		String str = "";
		for (int i = 0; i < strings.length; i++) {
			str += strings[i] + " ";
		}
		return str;
	}

}
