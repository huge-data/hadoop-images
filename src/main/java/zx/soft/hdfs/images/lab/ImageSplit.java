package zx.soft.hdfs.images.lab;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;

public class ImageSplit implements InputSplit {

	private int offset; //determines which byte in the overall image
	//to start reading data from
	private int height; //number of rows in the ImageSplit
	private int width; //number of columns in the ImageSplit
	int startRow; //row number of the first line in the InputSplit
	int numRows; //number of rows in the relevant part of the InputSplit

	public ImageSplit() {

	}

	public ImageSplit(int offset, int height, int width, int startRow, int numRows) {
		this.offset = offset;
		this.height = height;
		this.width = width;
		this.startRow = startRow;
		this.numRows = numRows;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		offset = in.readInt();
		height = in.readInt();
		width = in.readInt();
		startRow = in.readInt();
		numRows = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(offset);
		out.writeInt(height);
		out.writeInt(width);
		out.writeInt(startRow);
		out.writeInt(numRows);
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public int getHeight() {
		return height;
	}

	public void setHeight(int height) {
		this.height = height;
	}

	public int getWidth() {
		return width;
	}

	public void setWidth(int width) {
		this.width = width;
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

	@Override
	public long getLength() throws IOException {
		return height;
	}

	@Override
	public String[] getLocations() throws IOException {
		return new String[0];
	}

}
