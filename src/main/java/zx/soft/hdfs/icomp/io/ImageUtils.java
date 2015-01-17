package zx.soft.hdfs.icomp.io;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;

public class ImageUtils {

    public static Color getAverageColor(BufferedImage bitmap) {
        long redBucket = 0;
        long greenBucket = 0;
        long blueBucket = 0;
        long pixelCount = 0;
        for (int y = 0; y < bitmap.getHeight(); y++) {
            for (int x = 0; x < bitmap.getWidth(); x++) {
                Color c = new Color(bitmap.getRGB(x, y));
                pixelCount++;
                redBucket += c.getRed();
                greenBucket += c.getGreen();
                blueBucket += c.getBlue();
            }
        }
        return new Color((int) (redBucket / pixelCount), (int) (greenBucket / pixelCount),
                (int) (blueBucket / pixelCount));
    }

    public static int[] generateBWHistogram(BufferedImage bitmap, int bucketSize) {
        int[] histogram = new int[256 / bucketSize];
        for (int y = 0; y < bitmap.getHeight(); y++) {
            for (int x = 0; x < bitmap.getWidth(); x++) {
                Color c = new Color(bitmap.getRGB(x, y));
                int grey = (c.getRed() + c.getBlue() + c.getGreen()) / 3;
                int index = grey / bucketSize;
                histogram[index]++;
            }
        }
        return histogram;
    }

    // TODO configure split number
    public static BufferedImage[][] splitImage(BufferedImage img, int rows, int columns) {
        int chunkHeight = img.getHeight() / rows;
        int chunkWidth = img.getWidth() / columns;
        BufferedImage result[][] = new BufferedImage[rows][columns];
        for (int x = 0; x < rows; x++) {
            for (int y = 0; y < columns; y++) {
                BufferedImage chunk = new BufferedImage(chunkWidth, chunkHeight, img.getType());
                Graphics2D g = chunk.createGraphics();
                g.drawImage(img, 0, 0, chunkWidth, chunkHeight, chunkWidth * y, chunkHeight * x, chunkWidth * y
                        + chunkWidth, chunkHeight * x + chunkHeight, null);
                result[x][y] = chunk;
            }
        }
        return result;
    }

}
