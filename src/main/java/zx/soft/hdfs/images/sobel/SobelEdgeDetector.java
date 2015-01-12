package zx.soft.hdfs.images.sobel;

import java.awt.image.BufferedImage;
import java.io.IOException;

public class SobelEdgeDetector {

	public void process() throws IOException {

		int i, j;
		double Gx[][], Gy[][], G[][];

		BufferedImage inImg = getSourceImage();

		int imgWidth = inImg.getWidth();
		int imgHeight = inImg.getHeight();

		int[] pixelArr = new int[imgWidth * imgHeight];
		int[][] outArr = new int[imgWidth][imgHeight];

		inImg.getRaster().getPixels(0, 0, imgWidth, imgHeight, pixelArr);

		int counter = 0;

		for (i = 0; i < imgWidth; i++) {
			for (j = 0; j < imgHeight; j++) {

				outArr[i][j] = pixelArr[counter];
				counter = counter + 1;
			}
		}

		Gx = new double[imgWidth][imgHeight];
		Gy = new double[imgWidth][imgHeight];
		G = new double[imgWidth][imgHeight];

		for (i = 0; i < imgWidth; i++) {
			for (j = 0; j < imgHeight; j++) {
				if (i == 0 || i == imgWidth - 1 || j == 0 || j == imgHeight - 1)
					Gx[i][j] = Gy[i][j] = G[i][j] = 0; // Image boundary cleared 
				else {
					Gx[i][j] = outArr[i + 1][j - 1] + 2 * outArr[i + 1][j] + outArr[i + 1][j + 1]
							- outArr[i - 1][j - 1] - 2 * outArr[i - 1][j] - outArr[i - 1][j + 1];
					Gy[i][j] = outArr[i - 1][j + 1] + 2 * outArr[i][j + 1] + outArr[i + 1][j + 1]
							- outArr[i - 1][j - 1] - 2 * outArr[i][j - 1] - outArr[i + 1][j - 1];
					G[i][j] = Math.abs(Gx[i][j]) + Math.abs(Gy[i][j]);
				}
			}
		}
		counter = 0;
		for (int ii = 0; ii < imgWidth; ii++) {
			for (int jj = 0; jj < imgHeight; jj++) {
				//System.out.println(counter); 

				pixelArr[counter] = (int) G[ii][jj];
				counter = counter + 1;
			}
		}

		BufferedImage outImg = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_BYTE_GRAY);
		outImg.getRaster().setPixels(0, 0, imgWidth, imgHeight, pixelArr);
		setEdgesImage(outImg);

	}

	public SobelEdgeDetector() {

	}

	private BufferedImage sourceImage;
	private BufferedImage edgesImage;

	public BufferedImage getSourceImage() {
		return sourceImage;
	}

	public void setSourceImage(BufferedImage image) {
		sourceImage = image;
	}

	public BufferedImage getEdgesImage() {
		return edgesImage;
	}

	public void setEdgesImage(BufferedImage edgesImage) {
		this.edgesImage = edgesImage;
	}

}