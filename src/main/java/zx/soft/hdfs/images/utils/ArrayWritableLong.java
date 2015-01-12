package zx.soft.hdfs.images.utils;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;

public class ArrayWritableLong extends ArrayWritable {
	//Helper class to write a set of values in an array together

	public ArrayWritableLong() {
		super(LongWritable.class);
	}

	public ArrayWritableLong(LongWritable[] values) {
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
