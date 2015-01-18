package zx.soft.hdfs.face.detect;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CombineWholeFileRecordReader extends RecordReader<Text, BytesWritable> {

	private CombineFileSplit split;
	//	private TaskAttemptContext context;
	private final int index;
	private RecordReader<Text, BytesWritable> rr;

	public CombineWholeFileRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) {
		this.split = split;
		//		this.context = context;
		this.index = index;
		rr = new WholeFileRecordReader();
	}

	@Override
	public void initialize(InputSplit genricSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		this.split = (CombineFileSplit) genricSplit;
		//		this.context = context;

		if (null == rr) {
			rr = new WholeFileRecordReader();
		}

		FileSplit filSplit = new FileSplit(this.split.getPath(index), this.split.getOffset(index),
				this.split.getLength(index), this.split.getLocations());
		System.out.println(this.split.getPath(index));
		System.out.println(index);
		this.rr.initialize(filSplit, context);

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return rr.nextKeyValue();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return rr.getCurrentKey();
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return rr.getCurrentValue();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return rr.getProgress();
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}

}
