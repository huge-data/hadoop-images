package zx.soft.hdfs.images.gaussianblur;

import java.awt.Color;
import java.awt.image.BufferedImage;

/**
 * 
 * @author wgybzb
 *
 */
public class GaussianBlur {

	private double sigma;
	private BufferedImage sourceImage;
	private BufferedImage filtredImage;

	public BufferedImage getSourceImage() {
		return sourceImage;
	}

	public void setSourceImage(BufferedImage image) {
		sourceImage = image;
	}

	public BufferedImage getEdgesImage() {
		return filtredImage;
	}

	public void setEdgesImage(BufferedImage edgesImage) {
		this.filtredImage = edgesImage;
	}

	public void gaussianBlur(BufferedImage image, double sigma) {

		int height = image.getHeight(null);
		int width = image.getWidth(null);

		BufferedImage tempImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);

		BufferedImage filteredImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);

		//--->>
		int n = (int) (6 * sigma + 1);

		double[] window = new double[n];
		double s2 = 2 * sigma * sigma;

		window[(n - 1) / 2] = 1;
		for (int i = 0; i < (n - 1) / 2; i++) {
			window[i] = Math.exp(-i * i / s2);
			window[n - i - 1] = window[i];
		}

		//--->>
		for (int i = 0; i < width; i++) {
			for (int j = 0; j < height; j++) {
				double sum = 0;
				double[] colorRgbArray = new double[] { 0, 0, 0 };
				for (int k = 0; k < window.length; k++) {
					int l = i + k - (n - 1) / 2;
					if (l >= 0 && l < width) {
						Color imageColor = new Color(image.getRGB(l, j));
						colorRgbArray[0] = colorRgbArray[0] + imageColor.getRed() * window[k];
						colorRgbArray[1] = colorRgbArray[1] + imageColor.getGreen() * window[k];
						colorRgbArray[2] = colorRgbArray[2] + imageColor.getBlue() * window[k];
						sum += window[k];
					}
				}
				for (int t = 0; t < 3; t++) {
					colorRgbArray[t] = colorRgbArray[t] / sum;
				}
				Color tmpColor = new Color((int) colorRgbArray[0], (int) colorRgbArray[1], (int) colorRgbArray[2]);
				tempImage.setRGB(i, j, tmpColor.getRGB());
			}
		}

		//--->>
		for (int i = 0; i < width; i++) {
			for (int j = 0; j < height; j++) {

				double sum = 0;
				double[] colorRgbArray = new double[] { 0, 0, 0 };

				for (int k = 0; k < window.length; k++) {
					int l = j + k - (n - 1) / 2;
					if (l >= 0 && l < height) {
						Color imageColor = new Color(tempImage.getRGB(i, l));
						colorRgbArray[0] = colorRgbArray[0] + imageColor.getRed() * window[k];
						colorRgbArray[1] = colorRgbArray[1] + imageColor.getGreen() * window[k];
						colorRgbArray[2] = colorRgbArray[2] + imageColor.getBlue() * window[k];
						sum += window[k];
					}
				}

				for (int t = 0; t < 3; t++) {
					colorRgbArray[t] = colorRgbArray[t] / sum;
				}

				Color tmpColor = new Color((int) colorRgbArray[0], (int) colorRgbArray[1], (int) colorRgbArray[2]);
				filteredImage.setRGB(i, j, tmpColor.getRGB());
			}
		}

		//return filteredImage;
		setEdgesImage(filteredImage);
	}

	public void process() {
		this.gaussianBlur(sourceImage, sigma);
	}

}