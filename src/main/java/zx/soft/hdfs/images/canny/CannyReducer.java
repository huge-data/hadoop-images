package zx.soft.hdfs.images.canny;

import java.awt.image.BufferedImage;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CannyReducer extends Reducer<LongWritable, BufferedImage, LongWritable, LongWritable> {

	@Override
	public void reduce(LongWritable key, Iterable<BufferedImage> values, Context context) throws IOException,
			InterruptedException {

		FileSystem dfs = FileSystem.get(context.getConfiguration());
		Path newimgpath = new Path(context.getWorkingDirectory(), "" + key.get());
		dfs.createNewFile(newimgpath);
		FSDataOutputStream ofs = dfs.create(newimgpath);
		BufferedImage img = values.iterator().next();
		ImageIO.write(img, "JPG", ofs);
		context.write(key, new LongWritable(1));
	}

}
