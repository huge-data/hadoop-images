package zx.soft.hdfs.icomp.exp01;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import zx.soft.hdfs.icomp.io.ImageUtils;

public class FeatureExtractorMapper extends Mapper<Text, BytesWritable, Text, Text> {

	@Override
	protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
		byte[] rawImage = value.getBytes();
		BufferedImage image = ImageIO.read(new ByteArrayInputStream(rawImage));
		StringBuffer sb = new StringBuffer();
		appendHistogram(image, sb);
		sb.append(',');
		appendAverageColor(image, sb);
		context.write(key, new Text(sb.toString()));
	}

	private void appendHistogram(BufferedImage image, StringBuffer sb) {
		int[] histogram = ImageUtils.generateBWHistogram(image, 16);
		for (int i = 0; i < histogram.length; i++) {
			if (i != 0)
				sb.append(',');
			sb.append(String.valueOf(histogram[i]));
		}
	}

	private void appendAverageColor(BufferedImage image, StringBuffer sb) {
		Color averageColor = ImageUtils.getAverageColor(image);
		sb.append(averageColor.getAlpha());
		sb.append(',');
		sb.append(averageColor.getRed());
		sb.append(',');
		sb.append(averageColor.getGreen());
		sb.append(',');
		sb.append(averageColor.getBlue());
	}

}
