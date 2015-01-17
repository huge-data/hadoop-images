package zx.soft.hdfs.icomp.exp03;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.DecimalFormat;

import javax.imageio.ImageIO;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import zx.soft.hdfs.icomp.io.GQViewComparison;

public class SimilarityFinderMapper extends Mapper<Text, BytesWritable, Text, Text> {

	private static final String PIVOT_IMG_PATH = "/br/edu/ufam/icomp/exp03/pivotImage.jpg";
	private int[][][] pivotImageSignature;

	public SimilarityFinderMapper() {
		try {
			BufferedImage img = ImageIO.read(this.getClass().getResourceAsStream(PIVOT_IMG_PATH));
			pivotImageSignature = GQViewComparison.getImageSignature(img);
		} catch (IOException e) {
		}
	}

	@Override
	protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
		byte[] rawImage = value.getBytes();
		BufferedImage image = ImageIO.read(new ByteArrayInputStream(rawImage));
		double similarity = getDistanceFromPivotImage(image);
		DecimalFormat df = new DecimalFormat("0.000");
		context.write(new Text(df.format(similarity)), key);
	}

	private double getDistanceFromPivotImage(BufferedImage image) {
		return GQViewComparison.compareImages(pivotImageSignature, GQViewComparison.getImageSignature(image));
	}

}
