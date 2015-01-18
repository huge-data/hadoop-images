package zx.soft.hdfs.images.lab;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

public class ImageRecordReader implements RecordReader<IntWritable, ImageWritable> {

	JobConf conf;
	ImageSplit split;
	FSDataInputStream inputStream;
	boolean finished = false;

	public ImageRecordReader(JobConf conf, ImageSplit split) {
		this.conf = conf;
		this.split = split;
		// initializes an inputStream for the RecordReader to get data from
		try {
			FileSystem fileSys = FileInputFormat.getInputPaths(conf)[0].getFileSystem(conf);
			inputStream = fileSys.open(FileInputFormat.getInputPaths(conf)[0]);
		} catch (IOException e) {
			System.out.println("IOException in ImageRecordReader");
		}
	}

	@Override
	public void close() throws IOException {
		if (inputStream != null)
			inputStream.close();
	}

	@Override
	public IntWritable createKey() {
		return new IntWritable();
	}

	@Override
	public ImageWritable createValue() {
		return new ImageWritable();
	}

	@Override
	public long getPos() throws IOException {
		return split.getOffset();
	}

	@Override
	public float getProgress() throws IOException {
		if (finished)
			return 1;
		return 0;
	}

	@Override
	public boolean next(IntWritable key, ImageWritable value) throws IOException {
		if (finished == false) {
			// key determines the order splits will be collected in
			// did not simply use offset becuase there would be duplicates
			key.set(split.getOffset() + split.getStartRow() * split.getWidth());

			// reads height * width number of bytes from inputStream
			byte[] bytes = new byte[split.getHeight() * split.getWidth()];
			try {
				inputStream.readFully(split.getOffset(), bytes);
			} catch (IOException e) {
				System.out.println("IOException in next() of " + "ImageRecordReader");
			}

			// sets parameters of ImageWritable
			value.setWidth(split.getWidth());
			value.setHeight(split.getHeight());
			value.setStartRow(split.getStartRow());
			value.setNumRows(split.getNumRows());
			value.setBytes(bytes);

			finished = true;
			return true;
		}
		return false;
	}

}
