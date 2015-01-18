package zx.soft.hdfs.images.lab;

import java.io.File;

public class Test {

	public static void main(String args[]) throws Exception {
		PGMImage image = new PGMImage(new File("mdb001.pgm"));
		PGMImage image2 = PGMContrast.contrastEnhance(image);
		image2.write(new File("output_optimizedImage"));
	}

}
