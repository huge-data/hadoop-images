package zx.soft.hdfs.images.lab;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ImageReducer extends MapReduceBase implements
		Reducer<IntWritable, BytesWritable, IntWritable, BytesWritable> {

	@Override
	public void reduce(IntWritable key, Iterator<BytesWritable> itr,
			OutputCollector<IntWritable, BytesWritable> collector, Reporter reporter) throws IOException {
		while (itr.hasNext()) {
			BytesWritable writable = itr.next();

			collector.collect(key, writable);
		}
	}

}
