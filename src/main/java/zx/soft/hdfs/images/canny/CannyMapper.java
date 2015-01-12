package zx.soft.hdfs.images.canny;

import java.awt.image.BufferedImage;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class CannyMapper extends Mapper<LongWritable, BufferedImage, LongWritable, BufferedImage> {

	@Override
	public void map(LongWritable key, BufferedImage value, Context context) throws IOException, InterruptedException {

		System.out.println("map started");

		//create the detector 
		CannyEdgeDetector detector = new CannyEdgeDetector();

		//adjust its parameters as desired 
		detector.setLowThreshold(0.5f);
		detector.setHighThreshold(1f);

		//apply it to an image 
		detector.setSourceImage(value);
		detector.process();

		System.out.println("Edge Detected chunk " + key.get());

		BufferedImage edges = detector.getEdgesImage();

		if (edges == null) {
			System.out.println("edge detect made a null");
		}

		//context.write(key, edges);

		FileSystem dfs = FileSystem.get(context.getConfiguration());
		Path newimgpath = new Path(context.getWorkingDirectory(), context.getJobID().toString() + "/" + key.get());
		dfs.createNewFile(newimgpath);
		FSDataOutputStream ofs = dfs.create(newimgpath);
		ImageIO.write(edges, "jpg", ofs);
	}

}
