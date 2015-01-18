package zx.soft.hdfs.images.lab;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class ImageInputFormat extends FileInputFormat<IntWritable, ImageWritable> {

	@Override
	public ImageSplit[] getSplits(JobConf conf, int numsplits) {
		ArrayList<ImageSplit> splits = new ArrayList<ImageSplit>();
		// get the file to process
		Path p = ImageInputFormat.getInputPaths(conf)[0];
		try {
			FileSystem fileSys = p.getFileSystem(conf);
			FSDataInputStream inputStream = fileSys.open(p);

			// get header info to figure out how many ImageSplits to make
			ImageHeader header = PGMImage.getHeader(inputStream);
			int imgHeight = header.getHeight();
			int imgWidth = header.getWidth();
			int imgOffset = header.getOffset();
			// add the header as its own ImageSplit
			splits.add(new ImageSplit(0, 1, imgOffset, 0, 0));

			int linesPerSplit;
			int padding = 64; //default num of rows to use as padding
			int linesLeft = imgHeight; //lines in image not yet split up
			int position = 0; //current row being processed
			int aboveRows, belowRows; //num of rows above/below lines per split to
			//ensure edges of image are processed correctly

			// calculates the height of an ImageSplit, taking into account
			// the num rows needed above and below the image
			while (linesLeft > 0) {
				linesPerSplit = Math.min((int) Math.sqrt(imgHeight), Math.min(linesLeft, 18500000 / imgWidth));
				aboveRows = (position - padding < 0) ? position : padding;
				if (position + linesPerSplit + padding > imgHeight)
					belowRows = imgHeight - (position + linesPerSplit);
				else
					belowRows = padding;
				linesLeft -= linesPerSplit;

				// modifies the height until splitHeight % 32 == 1
				// so that dimensions are correct for contrast enhancement
				int splitHeight = linesPerSplit + aboveRows + belowRows;
				while (splitHeight % 32 != 1) {
					if (aboveRows > 0) {
						aboveRows++;
						splitHeight++;
						if (splitHeight % 32 == 1)
							break;
					}
					if (position + linesPerSplit + belowRows + 1 < imgHeight) {
						belowRows++;
						splitHeight++;
					}
				}

				ImageSplit split = new ImageSplit(imgOffset + (position - aboveRows) * imgWidth, linesPerSplit
						+ aboveRows + belowRows, imgWidth, aboveRows, linesPerSplit);

				splits.add(split);
				position += linesPerSplit;
			}

		} catch (Exception e) {

		}

		return splits.toArray(new ImageSplit[splits.size()]);

	}

	@Override
	public RecordReader<IntWritable, ImageWritable> getRecordReader(InputSplit split, JobConf jobConf, Reporter reporter)
			throws IOException {
		return new ImageRecordReader(jobConf, (ImageSplit) split);
	}

}
