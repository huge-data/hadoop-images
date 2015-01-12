package zx.soft.hdfs.images.equalize;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.util.StringTokenizer;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import zx.soft.hdfs.images.utils.ArrayWritableLong;
import zx.soft.hdfs.images.utils.InputFormatImg;

public class Equalize {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: histeq <in> <out>");
			System.exit(2);
		}
		BufferedImage img = null;
		FileSystem dfs = FileSystem.get(conf);
		Path dir = new Path(otherArgs[0]);
		FileStatus[] files = dfs.listStatus(dir);

		//String fname = null;
		conf.setInt("utils.imagerecordreader.overlapPixel", 0);
		Path fpath = null;

		for (FileStatus file : files) {
			if (file.isDir())
				continue;

			fpath = file.getPath();
			//fname = fpath.getName();
			System.out.println(fpath);
		}

		Path outdir = new Path(otherArgs[1]);
		if (dfs.exists(outdir))
			dfs.delete(outdir, true);

		Job job = new Job(conf, "Histogram equalization");

		job.setJarByClass(Equalize.class);
		job.setMapperClass(HistoMapper.class);
		job.setCombinerClass(HistoReducer.class);
		job.setReducerClass(HistoReducer.class);

		job.setInputFormatClass(InputFormatImg.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(ArrayWritableLong.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		boolean ret = job.waitForCompletion(true);

		Path reduceFile = new Path(outdir, "part-r-00000");
		FSDataInputStream fs = dfs.open(reduceFile);
		String str = null;
		float[] histogram = new float[256];
		str = fs.readLine();

		StringTokenizer tokenizer = new StringTokenizer(str);
		if (tokenizer.hasMoreTokens())
			tokenizer.nextToken();

		for (int i = 0; i < histogram.length && tokenizer.hasMoreTokens(); i++) {
			histogram[i] = Long.valueOf(tokenizer.nextToken()).longValue();
			if (i > 0)
				histogram[i] += histogram[i - 1];
		}

		Path imgpath = fpath;//new Path(dir, imgname);
		Path newimgpath = new Path(outdir, fpath.getName());
		if (dfs.exists(newimgpath))
			dfs.delete(newimgpath, false);

		dfs.createNewFile(newimgpath);
		FSDataOutputStream ofs = dfs.create(newimgpath);

		fs = dfs.open(imgpath);
		img = ImageIO.read(fs);

		float pixelNum = img.getWidth() * img.getHeight();

		for (int x = 0; x < img.getWidth(); x++) {
			for (int y = 0; y < img.getHeight(); y++) {
				int rgb = img.getRGB(x, y);
				int red = (rgb >> 16) & 0xFF;
				int green = (rgb >> 8) & 0xFF;
				int blue = rgb & 0xFF;
				float hsb[] = new float[3];
				Color.RGBtoHSB(red, green, blue, hsb);
				int ind = (int) (255.0 * hsb[2]);
				int newrgb = Color.HSBtoRGB(hsb[0], hsb[1], histogram[ind] / pixelNum);
				img.setRGB(x, y, newrgb);
			}
		}

		ImageIO.write(img, "jpg", ofs);
		ofs.close();
		System.exit(ret ? 0 : 1);
	}

}