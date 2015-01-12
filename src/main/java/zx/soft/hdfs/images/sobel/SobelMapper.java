package zx.soft.hdfs.images.sobel;

import java.awt.image.BufferedImage;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class SobelMapper extends Mapper<LongWritable, BufferedImage, LongWritable, BufferedImage> {

	@Override
	public void map(LongWritable key, BufferedImage value, Context context) throws IOException, InterruptedException {

		System.out.println("Map Phase started");

		//create the detector 
		SobelEdgeDetector edgeDetector = new SobelEdgeDetector();

		//adjust its parameters as desired 

		//apply it to an image 
		edgeDetector.setSourceImage(value);
		edgeDetector.process();

		System.out.println("Edge Detected chunk " + key.get());

		BufferedImage edges = edgeDetector.getEdgesImage();

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
