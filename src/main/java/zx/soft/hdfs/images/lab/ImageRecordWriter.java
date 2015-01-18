package zx.soft.hdfs.images.lab;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

public class ImageRecordWriter implements RecordWriter<IntWritable, BytesWritable> {

	DataOutputStream out;

	public ImageRecordWriter(DataOutputStream out) {
		this.out = out;
	}

	@Override
	public void close(Reporter reporter) throws IOException {
		out.close();
	}

	/*
	 * Writes the bytes of value to the given OutputStream
	 */
	@Override
	public void write(IntWritable key, BytesWritable value) throws IOException {
		out.write(value.getBytes(), 0, value.getLength());
	}

}
