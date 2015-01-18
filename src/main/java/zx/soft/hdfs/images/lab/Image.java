package zx.soft.hdfs.images.lab;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Performs I/O operations related to a single image.
 *
 * It is expected that each subclass understands a different image format.
 */
public abstract class Image {

	/** The height of the image, in pixels. */
	protected int height;

	/** The width of the image, in pixels. */
	protected int width;

	/** The pixel representation of the image in row major order, with the upper left hand pixel
	 * encoded (in an unspecified format) somehow starting at pixel[0][0]. */
	protected int[][] pixels;

	/** If the image was created from a file, this will have a value corresponding to the given
	 * file. If the image wasn't created from a file (say, its a modification of another image),
	 * then this field will be null. */
	protected File file;

	/**
	 * Create an image from the given file.
	 *
	 * @param file The file to attempt to parse as an image.
	 */
	public Image(File file) throws IOException, FileNotFoundException, ImageFormatException {
		this.file = file;
		pixels = null;
		height = width = 0;
	}

	/**
	 * Create an image from the pixels. Performs a deep copy of the pixels.
	 *
	 * The pixels are in row major order, with the encoded data for the upper left pixel starting
	 * at pixels[0][0] (some image formats will use 3+ ints per pixel, and some will use 1 or 2).
	 *
	 * @param pixels the image encoded in pixels, in row-major order with the upper hand pixel
	 * corresponding (or starting at) to pixel[0][0].
	 */
	public Image(int[][] pixels) throws IllegalArgumentException {
		if (pixels.length == 0)
			throw new IllegalArgumentException("Pixel array must not be empty!");

		this.pixels = pixels;
		height = width = 0;
		file = null;
	}

	/**
	 * Writes the image to the output stream. The format is specified by the implementiing subclass.
	 *
	 * @param out An I/O output stream.
	 */
	public abstract void write(OutputStream out) throws IOException;

	/**
	 * Convenience wrapper around void write(java.io.OutputSream).
	 *
	 * @param outFile the image is written to this output file.
	 */
	public void write(File outFile) throws FileNotFoundException, IOException {
		write(new BufferedOutputStream(new FileOutputStream(outFile)));
	}

	/**
	 * Return the pixel data for this image.
	 */
	public int[][] getPixels() {
		return pixels;
	}

	/**
	 * Return the input file.
	 */
	public File getFile() {
		return file;
	}

	/**
	 * Return the image height in pixels.
	 */
	public int getHeight() {
		return height;
	}

	/**
	 * Return the image width in pixels.
	 */
	public int getWidth() {
		return width;
	}
}
