package zx.soft.hdfs.images.read;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/*
 * This is similar to WholeFileInputFormat
 * Expect each image to go to one Mapper
 * You get key as NullWritable and the value as BytesWritable
 */
public class ImageFileImputFormat extends FileInputFormat<NullWritable, BytesWritable> {

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
		return new ImageFileRecordReader();
	}

}