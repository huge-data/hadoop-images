package zx.soft.hdfs.icomp.exp02;

import java.awt.image.BufferedImage;
import java.awt.image.RescaleOp;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SplitTransformerMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {

	@Override
	protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
		byte[] rawImage = value.getBytes();
		BufferedImage image = ImageIO.read(new ByteArrayInputStream(rawImage));
		RescaleOp rescaleOp = new RescaleOp(1.2f, 15, null);
		rescaleOp.filter(image, image);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ImageIO.write(image, "jpg", baos);
		context.write(key, new BytesWritable(baos.toByteArray()));
	}

}
