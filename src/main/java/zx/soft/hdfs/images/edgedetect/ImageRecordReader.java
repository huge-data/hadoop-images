package zx.soft.hdfs.images.edgedetect;

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

public class ImageRecordReader extends RecordReader<LongWritable, BufferedImage> {

	// Image information
	private String fileName = null;
	private MemoryCacheImageInputStream image = null;
	private ImageReader reader = null;

	// Key/Value pair
	private LongWritable key = null;
	private BufferedImage value = null;

	// Configuration parameters
	// By default use percentage for splitting
	boolean byPixel = false;
	static int overlapPercent = 0;
	static int sizePercent = 0;
	public static int overlapPixel = 0;
	public static int sizePixel = 1000;

	// splits
	int totalXSplits = 0;
	int totalYSplits = 0;
	int currX = 0;
	int currY = 0;
	int currentSplit = 0;
	int imgwidth = 0;
	int imgheight = 0;

	public int getImgwidth() {
		return imgwidth;
	}

	public int getImgheight() {
		return imgheight;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public BufferedImage getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if ((float) (totalXSplits * totalYSplits) == 0)
			return 0;
		float retval = (float) currentSplit / (float) (totalXSplits * totalYSplits);
		if (retval > 1)
			return 0;
		return retval;
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException,
			InterruptedException {
		// Get file split
		FileSplit split = (FileSplit) genericSplit;
		Configuration conf = context.getConfiguration();
		// Ensure that value is not negative
		overlapPixel = conf.getInt("overlapPixel", 0);
		if (overlapPixel < 0) {
			overlapPixel = 0;
		}

		// Open the file
		Path file = split.getPath();
		FileSystem fs = file.getFileSystem(conf);
		FSDataInputStream fileIn = fs.open(split.getPath());
		image = new MemoryCacheImageInputStream(fileIn);

		// Get filename to use as key
		fileName = split.getPath().getName().toString();

		Iterator<ImageReader> readers = ImageIO.getImageReaders(image);
		reader = readers.next();
		reader.setInput(image);
		imgwidth = reader.getWidth(0);
		imgheight = reader.getHeight(0);
		CalculateTotalSplits();

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (currY < imgheight && fileName != null) {
			key = new LongWritable(currentSplit);//new Text(fileName);

			if (imgwidth * imgheight <= sizePixel * sizePixel) {
				Rectangle rect = new Rectangle(0, 0, imgwidth, imgheight);
				ImageReadParam irp = new ImageReadParam();
				irp.setSourceRegion(rect);
				value = reader.read(0, irp);
				currY = imgheight;
			} else {
				value = getSubWindow();
			}
			currentSplit += 1;
			return true;
		}
		return false;
	}

	private BufferedImage getSubWindow() {
		int x = currX, y = currY;
		currX += sizePixel;
		if (currX >= imgwidth) {
			currX = 0;
			currY += sizePixel;
		}
		int width = Math.min(sizePixel + overlapPixel, imgwidth - x);
		int height = Math.min(sizePixel + overlapPixel, imgheight - y);

		Rectangle rect = new Rectangle(x, y, width, height);
		ImageReadParam irp = new ImageReadParam();
		irp.setSourceRegion(rect);
		try {
			return reader.read(0, irp);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	private void CalculateTotalSplits() {
		try {
			totalXSplits = (int) Math.ceil(reader.getWidth(0) / Math.min(sizePixel, reader.getWidth(0)));
			totalYSplits = (int) Math.ceil(reader.getHeight(0) / Math.min(sizePixel, reader.getHeight(0)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
