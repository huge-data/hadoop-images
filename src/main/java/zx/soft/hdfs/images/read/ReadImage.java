package zx.soft.hdfs.images.read;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencv.core.Core;
import org.opencv.core.CvType;
import org.opencv.core.Mat;

public class ReadImage extends Configuration implements Tool {

	public static class ReadImageMapper extends Mapper<NullWritable, BytesWritable, Text, IntWritable> {

		@Override
		public void map(NullWritable key, BytesWritable value, Context context) throws IOException,
				InterruptedException {

			//read the file name and file size
			int size = value.getLength();
			String name = ((FileSplit) context.getInputSplit()).getPath().getName();

			System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
			Mat mat = Mat.eye(3, 3, CvType.CV_8UC1);
			System.out.println("mat = " + mat.dump());

			Text fileName = new Text(name);
			IntWritable fileSize = new IntWritable(size);

			context.write(fileName, fileSize);
		}

	}

	@Override
	public void setConf(Configuration conf) {
		// Auto-generated method stub
	}

	@Override
	public Configuration getConf() {
		return new Configuration();
	}

	@Override
	public int run(String[] args) throws Exception {
		// Auto-generated method stub
		Job readImageJob = new Job(getConf(), "Read Images");
		readImageJob.setJarByClass(ReadImage.class);
		readImageJob.setMapperClass(ReadImageMapper.class);

		readImageJob.setInputFormatClass(ImageFileImputFormat.class);
		readImageJob.setMapOutputKeyClass(Text.class);
		readImageJob.setMapOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(readImageJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(readImageJob, new Path(args[1]));

		if (readImageJob.waitForCompletion(true))
			return 1;
		else
			return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ReadImage(), args);
	}

}
