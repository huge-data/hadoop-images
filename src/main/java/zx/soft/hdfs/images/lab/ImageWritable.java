package zx.soft.hdfs.images.lab;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ImageWritable implements Writable {

	int width; //num of cols in the split of the whole image 
	int height; //num of rows int he split of the whole image
	byte[] bytes; //holds bytes of data from the image
	int startRow; //offset row into the whole image
	int numRows; //number of rows in the split

	public ImageWritable() {

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		width = in.readInt();
		height = in.readInt();
		bytes = new byte[height * width];
		in.readFully(bytes);
		startRow = in.readInt();
		numRows = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(width);
		out.write(height);
		out.write(bytes);
		out.write(startRow);
		out.write(numRows);
	}

	public int getWidth() {
		return width;
	}

	public void setWidth(int width) {
		this.width = width;
	}

	public int getHeight() {
		return height;
	}

	public void setHeight(int height) {
		this.height = height;
	}

	public byte[] getBytes() {
		return bytes;
	}

	public void setBytes(byte[] bytes) {
		this.bytes = bytes;
	}

	public int getStartRow() {
		return startRow;
	}

	public void setStartRow(int startRow) {
		this.startRow = startRow;
	}

	public int getNumRows() {
		return numRows;
	}

	public void setNumRows(int numRows) {
		this.numRows = numRows;
	}
}
