package zx.soft.hdfs.videos.analysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ProcessVideo extends Configuration implements Tool {

	public static class ProcessVideoMapper extends Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text fileName, Text filePath, Context context) throws IOException, InterruptedException {

			//we create a file system object for interacting with the HDFS
			FileSystem fs = FileSystem.get(context.getConfiguration());
			//copy code from the predefined location to the local dir for execution
			try {
				fs.copyToLocalFile(new Path("/user/austin/code/code-dumb.py"), new Path("code.py"));
			} catch (Exception e) {
				context.write(fileName, new Text(e.getMessage()));
				Job thisJob = new Job(context.getConfiguration());
				thisJob.killJob();
				return;
			}

			//copy the video file to the working directory of the map task
			try {
				fs.copyToLocalFile(new Path(filePath.toString()), new Path(fileName.toString()));
			} catch (Exception e) {
				context.write(fileName, new Text(e.getMessage()));
				return;
			}

			//build a process using the python code and run it
			//the python code takes in one parameter - the fileName
			try {
				ProcessBuilder pb = new ProcessBuilder("python", "code.py", fileName.toString());
				Process p = pb.start();//start the process

				//read output from the process
				InputStreamReader isr = new InputStreamReader(p.getInputStream());
				BufferedReader in = new BufferedReader(isr);

				String processOutput;
				Text opencvResult = new Text();
				while ((processOutput = in.readLine()) != null) {
					opencvResult.set(processOutput);
					context.write(fileName, opencvResult);
				}
			} catch (Exception e) {
				context.write(fileName, new Text(e.getMessage()));
			}
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

		Job readImageJob = new Job(getConf(), "Video analysis job");
		readImageJob.setJarByClass(ProcessVideo.class);
		readImageJob.setMapperClass(ProcessVideoMapper.class);

		readImageJob.setInputFormatClass(PathFileInputFormat.class);
		readImageJob.setMapOutputKeyClass(Text.class);
		readImageJob.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(readImageJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(readImageJob, new Path(args[1]));

		if (readImageJob.waitForCompletion(true))
			return 1;
		else
			return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ProcessVideo(), args);
	}

}
