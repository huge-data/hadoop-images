package zx.soft.hdfs.images.lab;

/**
 * Applies various transformations to a PGMImage.
 */
public class PGMTransform {

	/**
	 * The reduce function that builds the Gaussian Pyramid. Takes an image encoded as pixels and
	 * returns a new image (encoded as pixels) which is about half the height and half the width
	 * of the original. Performs the downsampling by taking the average over a 5x5 area.
	 */
	public static int[][] reduce(int image[][]) {
		int height = image.length;
		int width = image[0].length;

		if (height % 2 == 0 || width % 2 == 0)
			throw new IllegalArgumentException("Image row and column dimensions must be odd!");

		int newHeight = (height - 1) / 2 + 1;
		int newWidth = (width - 1) / 2 + 1;

		double weight[] = { 0.05, 0.25, 0.40, 0.25, 0.05 };

		int reducedImage[][] = new int[newHeight][newWidth];

		for (int i = 0; i < newHeight; i++)
			for (int j = 0; j < newWidth; j++) {
				double avg = 0.0;
				for (int m = -2; m <= 2; m++)
					for (int n = -2; n <= 2; n++)
						avg += weight[m + 2] * weight[n + 2] * get(image, 2 * i + m, 2 * j + n);
				reducedImage[i][j] = (int) avg;
			}

		return reducedImage;
	}

	/**
	 * The expand function that does Gaussian Interpolation. Takes an image encoded as pixels and
	 * returns a new image that is about twice the height and twice the width of the original.
	 * Performs the upsampling by taking the average over a 5x5 area.
	 */
	public static int[][] expand(int image[][]) {
		int height = image.length;
		int width = image[0].length;

		int newHeight = 2 * (height - 1) + 1;
		int newWidth = 2 * (width - 1) + 1;

		double weight[] = { 0.05, 0.25, 0.40, 0.25, 0.05 };

		int expandedImage[][] = new int[newHeight][newWidth];

		for (int i = 0; i < newHeight; i++)
			for (int j = 0; j < newWidth; j++) {
				double avg = 0.0;
				for (int m = -2; m <= 2; m++)
					for (int n = -2; n <= 2; n++)
						if (((i - m) % 2) == 0 && ((j - n) % 2) == 0)
							avg += weight[m + 2] * weight[n + 2] * get(image, (i - m) / 2, (j - n) / 2);

				expandedImage[i][j] = (int) (4 * avg);
			}

		return expandedImage;
	}

	/**
	 * The q(x) function. Takes an image and brightens the dark gray areas, while leaving alone
	 * the already bright areas (low-pass filter).
	 */
	public static int[][] quantize(int image[][], double G, double p, int maxValue) {
		int height = image.length;
		int width = image[0].length;

		int newImage[][] = new int[height][width];

		for (int i = 0; i < height; i++)
			for (int j = 0; j < width; j++) {
				int x = image[i][j];
				if (x < 40)
					newImage[i][j] = (int) (G * x * Math.pow((1 - x / 30.0), p) + x);
				else
					newImage[i][j] = x;
				if (newImage[i][j] > maxValue)
					newImage[i][j] = maxValue;
			}

		return newImage;
	}

	/**
	 * Returns the difference of two images (A - B) for building the Laplacian Pyramid out of the
	 * Guassian Pyramid.
	 */
	public static int[][] subtract(int a[][], int b[][]) {
		int heightA = a.length;
		int widthA = a[0].length;

		int heightB = b.length;
		int widthB = b[0].length;

		if (heightA != heightB || widthA != widthB)
			throw new IllegalArgumentException("Mismatched dimensions");

		int diff[][] = new int[heightA][widthA];

		for (int i = 0; i < heightA; i++)
			for (int j = 0; j < widthA; j++)
				diff[i][j] = max(a[i][j] - b[i][j], 0);

		return diff;
	}

	/**
	 * Returns the sum of two images (A + B) for reconstructing the original image from the Gaussian
	 * Pyramid. Saturates individual values to max.
	 */
	public static int[][] add(int a[][], int b[][], int max) {
		int heightA = a.length;
		int widthA = a[0].length;

		int heightB = b.length;
		int widthB = b[0].length;

		if (heightA != heightB || widthA != widthB)
			throw new IllegalArgumentException("Mismatched dimensions");

		int sum[][] = new int[heightA][widthA];

		for (int i = 0; i < heightA; i++)
			for (int j = 0; j < widthA; j++)
				sum[i][j] = min(a[i][j] + b[i][j], max);

		return sum;
	}

	/**
	 * Edge cases? What edge cases?
	 */
	private final static int get(int image[][], int i, int j) {
		if (i >= 0 && j >= 0 && i < image.length && j < image[i].length)
			return image[i][j];

		return 0;
	}

	/**
	 * Returns the greater of two integers.
	 */
	private final static int max(int a, int b) {
		return a < b ? b : a;
	}

	/**
	 * Returns the smaller of two integers.
	 */
	private final static int min(int a, int b) {
		return a > b ? b : a;
	}

	/**
	 * Convenience function for quantize(). Returns the result of applying the quantize function
	 * to the given image. Does not modify the original image. Returns a new image.
	 */
	public static PGMImage applyQuantize(PGMImage g0, double gain, double boost) {
		return new PGMImage(g0.getWhiteValue(), PGMTransform.quantize(g0.getPixels(), gain, boost, g0.getWhiteValue()));
	}

	/**
	 * Convenience function for add(). Returns the result of adding (pixel-wise) two images
	 * together. Does not modify either argument. Returns a new image which is the sum of the two
	 * arguments.
	 */
	public static PGMImage applyAdd(PGMImage a, PGMImage b) {
		return new PGMImage(a.getWhiteValue(), PGMTransform.add(a.getPixels(), b.getPixels(), a.getWhiteValue()));
	}

	/**
	 * Convenience function for reduce(). Returns the result of applying the reduce step to the
	 * given image. Does not modify the original image. Returns a new image.
	 */
	public static PGMImage applyReduce(PGMImage g0) {
		return new PGMImage(g0.getWhiteValue(), PGMTransform.reduce(g0.getPixels()));
	}

	/**
	 * Convenience function for reduce(). Returns the result of applying the expand step to the
	 * given image. Does not modify the original image. Returns a new image.
	 */
	public static PGMImage applyExpand(PGMImage g0) {
		return new PGMImage(g0.getWhiteValue(), PGMTransform.expand(g0.getPixels()));
	}

	/**
	 * Convenience function for subtract(). Returns the result of subtracting (pixel-wise) a from b.
	 * Does not modify either argument. Returns a new image which is the difference of the two
	 * arguments.
	 */
	public static PGMImage applySubtract(PGMImage a, PGMImage b) {
		return new PGMImage(a.getWhiteValue(), PGMTransform.subtract(a.getPixels(), b.getPixels()));
	}

}
