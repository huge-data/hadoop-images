package zx.soft.hdfs.images.equalize;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import zx.soft.hdfs.images.utils.ArrayWritableLong;

public class HistoReducer extends Reducer<LongWritable, ArrayWritableLong, LongWritable, ArrayWritableLong> {

	@Override
	public void reduce(LongWritable key, Iterable<ArrayWritableLong> values, Context context) throws IOException,
			InterruptedException {

		// Initialize histogram array
		LongWritable[] histogram = new LongWritable[256];
		for (int i = 0; i < histogram.length; i++) {
			histogram[i] = new LongWritable();
		}

		// Sum the parts
		Iterator<ArrayWritableLong> it = values.iterator();
		while (it.hasNext()) {
			LongWritable[] part = (LongWritable[]) it.next().toArray();
			for (int i = 0; i < histogram.length; i++) {
				histogram[i].set(histogram[i].get() + part[i].get());
			}
		}
		context.write(key, new ArrayWritableLong(histogram));
	}

}