package zx.soft.hdfs.icomp.io;

import java.awt.Color;
import java.awt.image.BufferedImage;

/*
 * Method used on GQView (http://fossies.org/linux/misc/gqview-2.1.5.tar.gz:a/gqview-2.1.5/src/similar.c)
 */
public class GQViewComparison {

    public static int[][][] getImageSignature(BufferedImage img) {
        // Divide the image into a 32 x 32 grid (1024 rectangles)
        BufferedImage[][] chunks = ImageUtils.splitImage(img, 32, 32);

        // For each of the rectangle in the grid, average the red, green & blue color channels and store in 3
        // seperate 32 x 32 array. These 32 x 32 arrays represents the signature of the image.
        int[][][] signature = new int[3][32][32];
        for (int i = 0; i < chunks.length; i++) {
            for (int j = 0; j < chunks[i].length; j++) {
                Color c = ImageUtils.getAverageColor(chunks[i][j]);
                signature[0][i][j] = c.getRed();
                signature[1][i][j] = c.getGreen();
                signature[2][i][j] = c.getBlue();
            }
        }
        return signature;
    }

    // To compare two images, compute the signatures of both the images and calculate the average of the
    // corresponding array differences. i.e. similarity = 1 - (abs(red[img1] - red[img2]) + abs(blue[img1] -
    // blue[img2]) + abs(green[img1] - green[img2])) / 255 * 1024 * 3
    public static double compareImages(int[][][] firstSignature, int[][][] secondSignature) {
        int sim = 0;
        for (int i = 0; i < firstSignature[0].length; i++) {
            for (int j = 0; j < firstSignature[0][i].length; j++) {
                sim += Math.abs(firstSignature[0][i][j] - secondSignature[0][i][j]);
                sim += Math.abs(firstSignature[1][i][j] - secondSignature[1][i][j]);
                sim += Math.abs(firstSignature[2][i][j] - secondSignature[2][i][j]);
            }
        }
        return (1.0 - sim / (255.0 * 1024.0 * 3.0));
    }
}
