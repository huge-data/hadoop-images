package zx.soft.hdfs.icomp.exp03;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarityFinderReducer extends Reducer<Text, Text, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Iterator<Text> it = values.iterator();
		int counter = 0;
		while (it.hasNext()) {
			it.next();
			counter++;
		}
		context.write(key, new IntWritable(counter));
	}

}
