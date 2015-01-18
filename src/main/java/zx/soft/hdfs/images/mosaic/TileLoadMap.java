package zx.soft.hdfs.images.mosaic;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.opencv.core.Mat;
import org.opencv.imgproc.Imgproc;

public class TileLoadMap extends MapReduceBase implements
		Mapper<LongWritable, MatPyramidWritable, Text, MatTileWritable> {

	private String format;

	@Override
	public void configure(JobConf job) {
		super.configure(job);

		format = job.getStrings(HadoopMosaicScaler.CONF_IMAGE_FORMAT)[0];

		File libFile = new File("/");

		try {
			Path pathToLib = Utils.getPathToCachedFile(job, new Path(HadoopMosaicScaler.OPENCV_LIB).getName());
			libFile = new File(pathToLib.toString());
			System.load(libFile.getAbsolutePath());
		} catch (Exception e) {
			e.printStackTrace();
		} catch (UnsatisfiedLinkError e) {
			throw new UnsatisfiedLinkError("Failed to link: " + libFile.getAbsolutePath());
		}
	}

	@Override
	public void map(LongWritable key, MatPyramidWritable tile, OutputCollector<Text, MatTileWritable> output,
			Reporter reporter) throws IOException {

		Mat mat = tile.toMat();
		String line = tile.getConfig();
		Pattern pattern = Pattern.compile("(?<=\\{)[0-9\\-,]*(?=\\})"); // matches inside the brackets {1,2,43,231,2131,312}
		System.out.println("line: " + line);
		Matcher m = pattern.matcher(line);
		while (m.find()) {
			String group = m.group();
			PyramidBinTile pTile = PyramidBinTile.fromString(group, "");

			Mat scaledMat = new Mat(pTile.getScaledHeight(), pTile.getScaledWidth(), mat.type());
			Imgproc.resize(mat, scaledMat, scaledMat.size());

			if (pTile.getTileYInBin() < 0)
				throw new IOException("y<0: " + pTile.getTileYInBin());
			if (pTile.getTileXInBin() < 0)
				throw new IOException("x<0: " + pTile.getTileXInBin());
			if (pTile.getTileYInBin() + pTile.getTileHeightInBin() > scaledMat.rows()) {
				throw new IOException("y > upper_limit: y: " + pTile.getTileYInBin() + ", height: "
						+ pTile.getTileHeightInBin() + ", rows: " + scaledMat.rows());
			}
			if (pTile.getTileXInBin() + pTile.getTileWidthInBin() > scaledMat.cols()) {
				throw new IOException("x > upper_limit: x: " + pTile.getTileXInBin() + ", width: "
						+ pTile.getTileWidthInBin() + ", cols: " + scaledMat.cols());
			}
			if (pTile.getTileHeightInBin() <= 0) {
				throw new IOException("height <= 0: height: " + pTile.getTileHeightInBin() + ", bin height: "
						+ pTile.getBin().getImageHeight() + ", y in bin: " + pTile.getTileYInBin() + ", raw height: "
						+ pTile.getScaledHeight());
			}
			if (pTile.getTileWidthInBin() <= 0) {
				throw new IOException("width <= 0: width: " + pTile.getTileWidthInBin() + ", bin width: "
						+ pTile.getBin().getImageWidth() + ", x in bin: " + pTile.getTileXInBin() + ", raw width: "
						+ pTile.getScaledWidth() + ", scale: " + pTile.getBin().getScale() + ", row: "
						+ pTile.getBin().getRow() + " col: " + pTile.getBin().getCol());
			}

			scaledMat = scaledMat.submat(pTile.getTileYInBin(), pTile.getTileYInBin() + pTile.getTileHeightInBin(),
					pTile.getTileXInBin(), pTile.getTileXInBin() + pTile.getTileWidthInBin()).clone();

			ImagePyramidBin bin = pTile.getBin();

			collectScaledTile(scaledMat, bin.getScale(), bin.getRow(), bin.getCol(), pTile.getXInBin(),
					pTile.getYInBin(), pTile.getBin().getImageWidth(), pTile.getBin().getImageHeight(), output);

			scaledMat.release();
		}

	}

	private void collectScaledTile(Mat scaled, int level, int row, int col, int x, int y, int parentW, int parentH,
			OutputCollector<Text, MatTileWritable> output) throws IOException {
		MatTileWritable value = null;

		try {
			value = new MatTileWritable(scaled, format, x, y, parentW, parentH);
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (value != null) {
			Text key = new Text("scale_" + String.valueOf(level) + "_bin_" + String.valueOf(row) + "_"
					+ String.valueOf(col) + "." + format);

			output.collect(key, value);
		}
	}

}
