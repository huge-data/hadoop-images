package zx.soft.hdfs.images.lab;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ImageMapper extends MapReduceBase implements
		Mapper<IntWritable, ImageWritable, IntWritable, BytesWritable> {

	/*
	 * given a byte[], converts it to an int[][] of pixels
	 */
	private int[][] makePixelsArr(byte[] bytes, int height, int width) {
		int[][] pixels = new int[height][width];
		int byteNum = 0;
		for (int i = 0; i < height; i++) {
			for (int j = 0; j < width; j++) {
				pixels[i][j] = bytes[byteNum];
				if (pixels[i][j] < 0)
					pixels[i][j] += 256;
				byteNum++;
			}
		}

		return pixels;
	}

	@Override
	public void map(IntWritable key, ImageWritable value, OutputCollector<IntWritable, BytesWritable> collector,
			Reporter reporter) throws IOException {

		// headers are not contrast enhanced, just immediately collected
		if (key.get() == 0) {
			collector.collect(key, new BytesWritable(value.getBytes()));
			return;
		}

		// convert 1D bytes array to 2D pixels array and construct a PGMImage
		byte[] bytes = value.getBytes();
		int pixels[][] = makePixelsArr(bytes, value.getHeight(), value.getWidth());
		PGMImage image = new PGMImage(255, pixels);

		try {
			PGMImage enhancedImg = PGMContrast.contrastEnhance(image);
			// allow garbage collector to clean up unused memory
			pixels = null;
			image = null;
			System.gc();

			// get relevant part of enhanced image and store in new BytesWritable
			// convert 2D array of pixels back into 1D array of bytes
			int[][] newPixels = enhancedImg.getPixels();
			byte[] newBytes = new byte[value.getNumRows() * value.getWidth()];
			int byteCount = 0;
			for (int i = value.getStartRow(); i < value.getStartRow() + value.getNumRows(); i++) {
				for (int j = 0; j < value.getWidth(); j++) {
					newBytes[byteCount] = (byte) newPixels[i][j];
					byteCount++;
				}
			}
			BytesWritable output = new BytesWritable(newBytes);

			collector.collect(key, output);

		} catch (ImageFormatException e) {
			e.printStackTrace();
		}
	}
}
