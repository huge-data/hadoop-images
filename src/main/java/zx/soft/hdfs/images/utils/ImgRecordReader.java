package zx.soft.hdfs.images.utils;

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.stream.MemoryCacheImageInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ImgRecordReader extends RecordReader<LongWritable, BufferedImage> {
	// Image information

	private String fileName = null;
	private ImageReader reader = null;

	private MemoryCacheImageInputStream image = null;
	// Key/Value pair
	private LongWritable key = null;
	private BufferedImage value = null;

	// Configuration parameters
	// By default use percentage for splitting
	boolean splittusingPixel = false;

	// splits
	int totalXSplits = 0;
	int totalYSplits = 0;
	int loctX = 0;
	int loctY = 0;
	int currentSplit = 0;
	int imgwidth = 0;
	int imgheight = 0;

	public int getImgheight() {
		return imgheight;
	}

	static int overlapPercent = 0;
	static int sizePercent = 0;

	@Override
	public void close() throws IOException {
		//nothing here
	}

	public int getImgwidth() {
		return imgwidth;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public BufferedImage getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	public static int overlapPixel = 0;

	@Override
	public float getProgress() throws IOException, InterruptedException {

		if ((float) (totalXSplits * totalYSplits) == 0) {
			return 0;
		}
		float retval = (float) currentSplit / (float) (totalXSplits * totalYSplits);

		if (retval > 1) {
			return 0;
		}
		return retval;
	}

	public static int sizePixel = 1000;

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException,
			InterruptedException {
		// Get file split

		FileSplit chunk = (FileSplit) genericSplit;
		Configuration conf = context.getConfiguration();
		// Ensure that value is not negative
		overlapPixel = conf.getInt("overlapPixel", 0);
		if (overlapPixel < 0) {
			overlapPixel = 0;
		}

		// Open the file
		Path file = chunk.getPath();
		FileSystem fs = file.getFileSystem(conf);
		FSDataInputStream fileIn = fs.open(chunk.getPath());
		image = new MemoryCacheImageInputStream(fileIn);

		// Get filename to use as key
		fileName = chunk.getPath().getName().toString();

		Iterator<ImageReader> imgrdr = ImageIO.getImageReaders(image);
		reader = imgrdr.next();
		reader.setInput(image);
		imgwidth = reader.getWidth(0);
		imgheight = reader.getHeight(0);
		findTotalSplits();

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (loctY < imgheight && fileName != null) {
			key = new LongWritable(currentSplit);//new Text(fileName);

			if (imgwidth * imgheight <= sizePixel * sizePixel) {
				Rectangle rect = new Rectangle(0, 0, imgwidth, imgheight);
				ImageReadParam irp = new ImageReadParam();
				irp.setSourceRegion(rect);
				value = reader.read(0, irp);
				loctY = imgheight;
			} else {
				value = getChunk();
			}
			currentSplit += 1;
			return true;
		}
		return false;
	}

	private BufferedImage getChunk() {
		int x = loctX, y = loctY;
		loctX += sizePixel;

		if (loctX >= imgwidth) {
			loctX = 0;
			loctY += sizePixel;
		}
		int width = Math.min(sizePixel + overlapPixel, imgwidth - x);
		int height = Math.min(sizePixel + overlapPixel, imgheight - y);

		Rectangle rect = new Rectangle(x, y, width, height);
		ImageReadParam irp = new ImageReadParam();
		irp.setSourceRegion(rect);

		try {
			return reader.read(0, irp);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	private void findTotalSplits() {
		try {
			totalXSplits = (int) Math.ceil(reader.getWidth(0) / Math.min(sizePixel, reader.getWidth(0)));
			totalYSplits = (int) Math.ceil(reader.getHeight(0) / Math.min(sizePixel, reader.getHeight(0)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
