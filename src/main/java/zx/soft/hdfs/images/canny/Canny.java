package zx.soft.hdfs.images.canny;

import java.awt.image.BufferedImage;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.MemoryCacheImageInputStream;

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

import zx.soft.hdfs.images.utils.ImgRecordReader;
import zx.soft.hdfs.images.utils.InputFormatImg;

public class Canny {

	public static void main(String[] args) throws Exception {

		Configuration config = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: Canny <in> <out>");
			System.exit(2);
		}

		BufferedImage img = null;
		FileSystem dfs = FileSystem.get(config);
		Path dir = new Path(otherArgs[0]);
		FileStatus[] files = dfs.listStatus(dir);

		config.setInt("overlapPixel", 64);

		int overlapPixel = ImgRecordReader.overlapPixel;
		System.out.println(overlapPixel);

		Path filepath = null;
		for (FileStatus file : files) {
			if (file.isDir())
				continue;
			filepath = file.getPath();

			System.out.println(filepath);
		}

		Path outdir = new Path(otherArgs[1]);
		if (dfs.exists(outdir))
			dfs.delete(outdir, true);
		Path workdir = dfs.getWorkingDirectory();

		Job job = new Job(config, "Canny Edge detection");
		job.setJarByClass(Canny.class);
		job.setMapperClass(CannyMapper.class);

		job.setReducerClass(CannyReducer.class);

		job.setInputFormatClass(InputFormatImg.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		boolean ret = job.waitForCompletion(true);
		String s = job.getTrackingURL();
		Path tmpdir = new Path(workdir, s.substring(s.indexOf("jobid") + 6));

		int i = 0;
		Path iPath = new Path(tmpdir, "" + i);
		int currX = 0, currY = 0;
		int sizePixel = ImgRecordReader.sizePixel;
		int border = 16;

		FSDataInputStream filesys = null;
		MemoryCacheImageInputStream image = new MemoryCacheImageInputStream(dfs.open(filepath));
		Iterator<ImageReader> readers = ImageIO.getImageReaders(image);
		ImageReader reader = readers.next();
		reader.setInput(image);
		int imgwidth = 0, imgheight = 0;
		imgwidth = reader.getWidth(0);
		imgheight = reader.getHeight(0);

		img = new BufferedImage(imgwidth, imgheight, BufferedImage.TYPE_INT_RGB);

		if (imgwidth * imgheight <= sizePixel * sizePixel) {
			filesys = dfs.open(iPath);
			img = ImageIO.read(filesys);
			iPath = null;
		}
		while (iPath != null && dfs.exists(iPath)) {
			int x = currX, y = currY;
			currX += sizePixel;
			if (currX >= imgwidth) {
				currX = 0;
				currY += sizePixel;
			}

			filesys = dfs.open(iPath);
			BufferedImage window = ImageIO.read(filesys);
			int width = window.getWidth() - border * 2;
			int height = window.getHeight() - border * 2;

			img.setRGB(x + border, y + border, width, height,
					window.getRGB(border, border, width, height, null, 0, width), 0, width);
			filesys.close();
			i++;
			iPath = new Path(tmpdir, "" + i);
		}
		Path newimgpath = new Path(outdir, filepath.getName());

		if (dfs.exists(newimgpath)) {
			dfs.delete(newimgpath, false);
		}

		dfs.createNewFile(newimgpath);
		FSDataOutputStream ofs = dfs.create(newimgpath);
		ImageIO.write(img, "JPG", ofs);
		ofs.close();
		dfs.delete(tmpdir, true);

		System.exit(ret ? 0 : 1);
	}

}
