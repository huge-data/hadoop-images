package zx.soft.hdfs.images.equalize;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import zx.soft.hdfs.images.utils.ArrayWritableLong;

public class HistoMapper extends Mapper<LongWritable, BufferedImage, LongWritable, ArrayWritableLong> {

	private final static LongWritable one = new LongWritable(1);

	@Override
	public void map(LongWritable key, BufferedImage value, Context context) throws IOException, InterruptedException {

		// Initialize histogram array
		LongWritable[] histogram = new LongWritable[256];
		for (int i = 0; i < histogram.length; i++) {
			histogram[i] = new LongWritable();
		}

		for (int x = 0; x < value.getWidth(); x++) {
			for (int y = 0; y < value.getHeight(); y++) {
				int rgb = value.getRGB(x, y);
				int red = (rgb >> 16) & 0xFF;
				int green = (rgb >> 8) & 0xFF;
				int blue = rgb & 0xFF;
				float hsb[] = new float[3];
				Color.RGBtoHSB(red, green, blue, hsb);
				int ind = (int) (255.0 * hsb[2]);
				histogram[ind].set(histogram[ind].get() + 1);
			}
		}

		context.write(one, new ArrayWritableLong(histogram));
	}

}