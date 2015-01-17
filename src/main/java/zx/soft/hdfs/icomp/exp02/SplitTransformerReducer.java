package zx.soft.hdfs.icomp.exp02;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SplitTransformerReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {

	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException,
			InterruptedException {
		Iterator<BytesWritable> it = values.iterator();
		while (it.hasNext()) {
			context.write(key, it.next());
		}
	}

}
