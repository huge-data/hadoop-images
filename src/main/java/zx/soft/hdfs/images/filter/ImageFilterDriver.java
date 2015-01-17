package zx.soft.hdfs.images.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author wgybzb
 *
 */
public class ImageFilterDriver extends Configured implements Tool {

	public static void main(String args[]) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new ImageFilterDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String args[]) throws Exception {

		//System.out.println("Hello World! 2");
		if (args.length != 2) {
			System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n", getClass().getSimpleName());
			System.exit(-1);
		}

		Job job = new Job();
		//		Configuration conf = new Configuration();
		//conf.set("mapreduce.jobtracker.address", "local");
		//conf.set("fs.defaultFS", "file:///");

		job.setJarByClass(ImageFilterDriver.class);

		job.setJobName("Image Face Detection");
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(CombineWholeFileInputFormat.class);

		job.setMapperClass(ResizeMapper.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);

		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
		return 0;
	}

}
