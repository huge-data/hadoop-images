package zx.soft.hdfs.images.lab;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

/**
 * The Netpbm greyscale image format is dead simple to understand. Each .pgm file contains a bit of
 * metadata, and then one or two bytes per pixel, which it can get away with since a black and white
 * format. The only tricky bit is that one file can contain multiple images encoded this way. This
 * ignores that part of the specification, and can only parse the first image in a file.
 * Additionally, this class only understands the raw PGM format, not the plain text format (the
 * magic bits must start with P5), and the max white value must be less than or equal to 255.
 *
 * Whether or not it uses two bytes per pixel or one byte is determined by a special white pixel
 * value. A pixel that has this value is white, so it effectively sets the upper limit for a pixel
 * value. Zero, which is the lower limit, always indicates black. Values inbetween indicate some
 * shade of grey.
 *
 * You can read more about the PGM format here:
 * http://netpbm.sourceforge.net/doc/pgm.html
 */
public class PGMImage extends Image {

	/** A pixel that is this greyscale is white. (A pixel that is 0 in greyscale is black). */
	private int whiteValue;

	/**
	 * Create an image from the given file.
	 *
	 * @param file The file to attempt to parse as an image.
	 */
	public PGMImage(File file) throws IOException, FileNotFoundException, ImageFormatException {
		super(file);

		this.file = file;
		InputStream inStream = new FileInputStream(file);
		parseBytes(inStream);
		inStream.close();
	}

	/**
	 * Create a PGM image from scratch, passing in the pixel values. Pixels must be in row-major
	 * order, with the upper left pixel corresponding to pixel[0][0].
	 *
	 * @param whiteValue a pixel that has this value is displayed as white. this is also the upper
	 * limit for a value that a pixel can have.
	 * @param pixels the image encoded in pixels.
	 */
	public PGMImage(int whiteValue, int pixels[][]) throws IllegalArgumentException {
		super(pixels);

		if (whiteValue < 0 || whiteValue >= 256)
			throw new IllegalArgumentException("White value must be between 0 and 255");

		this.file = null;
		this.height = pixels.length;
		this.width = pixels[0].length;
		this.whiteValue = whiteValue;
		this.pixels = new int[height][width];

		/* check that each pixel is less than the white value and make a deep copy of the pixel
		 * array .*/
		/* check that each pixel is less than the white value and make a deep copy of the pixel
		*          * array .*/
		this.pixels = pixels;
		// for(int i = 0; i < height; i++)
		//         // {
		//                 //     for(int j = 0; j < width; j++)
		//                         //     {
		//                                 //         if(pixels[i][j] < 0 || pixels[i][j] > whiteValue)
		//                                         //         {
		//                                                 //             throw new IllegalArgumentException("Greyscale value of pixel at pixel["
		//                                                         //                     + i + "][" + j + "] = " + pixels[i][j] + " which is negative or greater"
		//                                                                 //                     + " than value of the white value: " + whiteValue);
		//                                                                         //         }
		//
		//                                                                                 //         this.pixels[i][j] = pixels[i][j];
		//                                                                                         //     }
		//                                                                                                 // }
	}

	public static ImageHeader getHeader(InputStream in) throws IOException, ImageFormatException {

		PGMInputBuffer inStream = new PGMInputBuffer(in);
		int width;
		int height;
		int whiteValue;
		int offset;

		int magicNums[] = new int[2];
		inStream.readMagicNumber(magicNums);

		if (magicNums[0] != 0x50 || magicNums[1] != 0x35)
			throw new ImageFormatException("Wrong magic numbers for a PGM file: "
					+ java.util.Arrays.toString(magicNums));

		inStream.skipWhitespace();
		inStream.skipComments();
		width = inStream.readASCIIInt();

		inStream.skipWhitespace();
		height = inStream.readASCIIInt();

		inStream.skipWhitespace();
		whiteValue = inStream.readASCIIInt();

		if (whiteValue < 0 || whiteValue >= Math.pow(2, 16))
			throw new ImageFormatException("Invalid max grey value: " + whiteValue);

		int c = inStream.read();

		if (!Character.isWhitespace(c))
			throw new ImageFormatException("Was expecting a whitespace character, got: " + c);

		if (c == -1)
			throw new ImageFormatException("Was expecting a whitespace character, got EOF.");

		//offset is where the actual image begins
		offset = inStream.getPosition();

		return new ImageHeader(offset, height, width);
	}

	/**
	 * Parse the image data. This is a rough guide to the format:
	 *
	 * fields:
	 *  1. P5 (magic number)
	 *  2. (some whitespace)
	 *  2a. # (some comment) followed by a newline
	 *  3. width in pixels as an ascii string
	 *  4. (some whitespace)
	 *  5. height in pixels as as ascii string
	 *  6. (some whitespace)
	 *  7. the maximum white value (see above for explanation) as an ascii string
	 *  8. (some whitespace, probably a newline)
	 *  9. pixels in row-major order, from top to bottom.
	 *
	 * example:
	 *
	 * $ xxd test.images/mdb010.pgm | head -n 2
	 * 0000000: 5035 0a32 3034 390a 3230 3439 0a32 3535  P5.2049.2049.255
	 * 0000010: 0a00 0000 0000 0000 0000 0000 0000 0000  ................
	 *
	 * or if it includes a comment, like this:
	 * $ xxd test.images/mdb021.pgm | head -n 5
	 * 0000000: 5035 0a23 2043 5245 4154 4f52 3a20 4749  P5.# CREATOR: GI
	 * 0000010: 4d50 2050 4e4d 2046 696c 7465 7220 5665  MP PNM Filter Ve
	 * 0000020: 7273 696f 6e20 312e 310a 3230 3439 2032  rsion 1.1.2049 2
	 * 0000030: 3034 390a 3235 350a 0000 0000 0000 0000  049.255.........
	 * 0000040: 0000 0000 0000 0000 0000 0000 0000 0000  ................
	 */
	protected void parseBytes(InputStream in) throws IOException, ImageFormatException {
		/* Create a buffered input stream so we can go backwards in the buffer via mark() and
		 * reset(). Note that the PGMInputBuffer is a wrapper around a BufferedInputStream.
		 * See below. */
		PGMInputBuffer inStream = new PGMInputBuffer(in);

		int magicNums[] = new int[2];
		inStream.readMagicNumber(magicNums);

		if (magicNums[0] != 0x50 || magicNums[1] != 0x35)
			throw new ImageFormatException("Wrong magic numbers for a PGM file: "
					+ java.util.Arrays.toString(magicNums) + "\nAre you sure " + file.getAbsolutePath()
					+ " is a valid .pgm file?");

		inStream.skipWhitespace();
		inStream.skipComments();
		width = inStream.readASCIIInt();

		inStream.skipWhitespace();
		height = inStream.readASCIIInt();

		inStream.skipWhitespace();
		whiteValue = inStream.readASCIIInt();

		if (whiteValue < 0 || whiteValue >= Math.pow(2, 16))
			throw new ImageFormatException("Invalid max grey value: " + whiteValue);

		int c = inStream.read();

		if (!Character.isWhitespace(c))
			throw new ImageFormatException("Was expecting a whitespace character, got: " + c);

		if (c == -1)
			throw new ImageFormatException("Was expecting a whitespace character, got EOF.");

		pixels = new int[height][width];
		inStream.readPixels(width, height, whiteValue < 256 ? 1 : 2, pixels);
	}

	@Override
	public int[][] getPixels() {
		return pixels;
	}

	/**
	 * Writes the current image to file.
	 */
	@Override
	public void write(File file) throws FileNotFoundException, IOException {
		write(new BufferedOutputStream(new FileOutputStream(file)));
	}

	/**
	 * Writes the current image to the output stream.
	 *
	 * @param outStream The output stream to write the image to.
	 */
	@Override
	public void write(OutputStream outStream) throws FileNotFoundException, IOException {
		Charset ASCII = Charset.forName("US-ASCII");

		// magic number + whitespace (newline)
		outStream.write((byte) 0x50);
		outStream.write((byte) 0x35);
		outStream.write((byte) 0xa);

		// width + newline
		byte widthBytes[] = Integer.toString(width).getBytes(ASCII);
		outStream.write(widthBytes);
		outStream.write((byte) 0xa);

		// height + newline
		byte heightBytes[] = Integer.toString(height).getBytes(ASCII);
		outStream.write(heightBytes);
		outStream.write((byte) 0xa);

		// max grey value + newline
		byte whiteValueBytes[] = Integer.toString(whiteValue).getBytes(ASCII);
		outStream.write(whiteValueBytes);
		outStream.write((byte) 0xa);

		// finally, the pixels themselves in row order
		if (whiteValue < 256) {
			for (int i = 0; i < height; i++)
				for (int j = 0; j < width; j++)
					outStream.write((byte) pixels[i][j]);
		} else {
			for (int i = 0; i < height; i++)
				for (int j = 0; j < width; j++) {
					// higher order bytes go first
					outStream.write((byte) ((pixels[i][j] >> 8) & 0xFF));
					outStream.write((byte) pixels[i][j]);
				}
		}

		outStream.flush();
	}

	/**
	 * Returns the greyscale value that corresponds to white.
	 */
	public int getWhiteValue() {
		return whiteValue;
	}

	/**
	 * A simple wrapper around a BufferedInputStream.
	 */
	private static class PGMInputBuffer extends BufferedInputStream {
		public PGMInputBuffer(InputStream in) {
			super(in);
		}

		public int getPosition() {
			return pos;
		}

		/**
		 * * Reads the first two bytes that make up the magic number from the input buffer.
		 */
		public void readMagicNumber(int magicNums[]) throws IOException, ImageFormatException {
			if (magicNums.length < 2)
				throw new IllegalArgumentException("Array to store magic numbers is too small");

			for (int i = 0; i < 2; i++) {
				magicNums[i] = read();

				if (magicNums[i] == -1)
					throw new ImageFormatException("File too short: missing magic numbers");
			}

			return;
		}

		/**
		 * Advances the input buffer over any whitespace, so that the next character to be read
		 * is not whitespace.
		 */
		public void skipWhitespace() throws IOException, ImageFormatException {
			int c = ' ';
			while (Character.isWhitespace((char) c)) {
				mark(2);
				c = read();

				if (c == -1)
					throw new ImageFormatException("Ran out of file while reading to end of whitespace");
			}

			reset();
		}

		/**
		 * Advances the input buffer over any comments
		 */
		public void skipComments() throws IOException {
			int endl = '\n';

			mark(2);
			int c = read();

			if (c != '#') {
				reset();
				return;
			}

			while ((c = read()) != -1 && c != endl)
				;
		}

		/**
		 * Reads one ASCII int from the input buffer, stopping at the first non-digit character.
		 */
		public int readASCIIInt() throws IOException, ImageFormatException {
			StringBuffer intBuffer = new StringBuffer();

			mark(2);
			int c = read();

			while (Character.isDigit((char) c)) {
				intBuffer.append((char) c);
				mark(2);
				c = read();
			}

			if (c == -1)
				throw new ImageFormatException("Ran out of file while reading an ASCII integer");

			reset();

			try {
				return Integer.parseInt(intBuffer.toString());
			} catch (NumberFormatException e) {
				throw new ImageFormatException("Malformed ASCII integer");
			}
		}

		/**
		 * Reads width * height pixels from the input buffer into the passed array, in row-column
		 * notation. The array[0][0] will correspond to the upper left hand pixel upon completion.
		 * If the max grey value is less than 255, then each pixel is one byte. Otherwise each pixel
		 * is two bytes.
		 */
		public void readPixels(int width, int height, int bytesPerPixel, int[][] pixels) throws IOException,
				ImageFormatException, IllegalArgumentException {
			int numPixels = width * height;
			if (bytesPerPixel == 1) // pixels are one byte
			{
				for (int i = 0; i < numPixels; i++) {
					int pixel = read();

					if (pixel == -1)
						throw new ImageFormatException("Was expecting " + numPixels + " pixels, got " + i
								+ " before reaching the end of the file");

					pixels[i / width][i % width] = pixel;
				}
			} else if (bytesPerPixel == 2) // pixels are two bytes, most significant byte comes first
			{
				for (int i = 0; i < numPixels; i++) {
					int p1 = read();

					if (p1 == -1)
						throw new ImageFormatException("Was expecting " + numPixels + " pixels, got" + i
								+ " before reaching the end of the file");

					int p2 = read();

					if (p1 == -1)
						throw new ImageFormatException("Was expecting " + numPixels + " pixels, got" + i
								+ " before reaching the end of the file");

					pixels[i / width][i % width] = (p1 << 8) | (p2);
				}
			} else {
				throw new IllegalArgumentException("Bytes per pixel must be one or two");
			}
		}
	}
}
