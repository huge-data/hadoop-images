package zx.soft.hdfs.images.lab;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * The main class for the contrast enhancer. 
 *
 * The arguments should be an input directory full of .pgm images (one image per file), and the
 * output directory should be a directory that does not currently exist which will contain the
 * enhanced images upon completion.
 *
 * Usage: java driver.Driver <input directory> <output directory>
 */
public class Driver {

	/**
	 * The main driver for the contrast enhancer.
	 *
	 * Verifies that the input directory exists, and that the output directory doesn't exist. Then
	 * tries to apply the contrast enhancement to each of the files inside the input directory.
	 *
	 * @param args should be two arguments, the first being the input directory and the second being
	 * the output directory. the second directory should not exist.
	 */
	public static void main(String[] args) throws IOException, ImageFormatException {
		if (args.length != 3) {
			System.out.println("Usage: java driver.Driver <input directory>" + " <output directory>");
			System.out.println("Jarfile Usage: java -jar $jarfile <input directory>" + " <output directory>");
			System.exit(0);
		}

		/* verify that input directory exists and the output directory doesn't. */

		JobConf job = new JobConf(Driver.class);
		job.set("mapred.child.java.opts", "-Xmx1500m");
		job.setJarByClass(Driver.class);
		job.setInputFormat(ImageInputFormat.class);
		job.setOutputFormat(ImageOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setMapperClass(ImageMapper.class);
		job.setReducerClass(ImageReducer.class);
		FileInputFormat.addInputPaths(job, args[1]);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		JobClient.runJob(job);
	}
}
