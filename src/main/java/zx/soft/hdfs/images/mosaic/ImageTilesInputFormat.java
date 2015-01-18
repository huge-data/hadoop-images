package zx.soft.hdfs.images.mosaic;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * Created by roinir on 12/06/2014.
 */
public class ImageTilesInputFormat extends CombineFileInputFormat<LongWritable, MatPyramidWritable> {

	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	@Override
	public RecordReader<LongWritable, MatPyramidWritable> getRecordReader(InputSplit split, JobConf job,
			Reporter reporter) throws IOException {
		return new ImageTilesRecordReader((CombineFileSplit) split, job);
	}

}
