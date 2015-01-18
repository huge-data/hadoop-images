package zx.soft.hdfs.images.mosaic;

import org.opencv.core.Mat;
import org.opencv.highgui.Highgui;

import java.awt.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MatTileWritable extends MatWritable{

    private int x;
    private int y;
    private int parentWidth;
    private int parentHeight;

    public int getParentWidth() {
        return parentWidth;
    }

    public int getParentHeight() {
        return parentHeight;
    }



    public MatTileWritable(){
        super();
    }

    public MatTileWritable(Mat mat, String format, int x, int y, int parentWidth, int parentHeight) throws IOException{
        super(mat,format);
        this.x = x;
        this.y = y;
        this.parentWidth = parentWidth;
        this.parentHeight = parentHeight;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);
        dataOutput.writeInt(x);
        dataOutput.writeInt(y);
        dataOutput.writeInt(parentWidth);
        dataOutput.writeInt(parentHeight);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
        x = dataInput.readInt();
        y = dataInput.readInt();
        parentWidth = dataInput.readInt();
        parentHeight = dataInput.readInt();
    }

    public int getX() {
        return x;
    }

    public int getY() {
        return y;
    }

    public void setX(int x) {
        this.x = x;
    }

    public void setY(int y) {
        this.y = y;
    }

    /**
     * Checks if this segment of the image is in an area in coordinate space defined by range.
     * @param range defines the part of the whole image of interest
     * @return true if range and the area defined by the segment intersect. False otherwise.
     */
    public boolean isInRange(Rectangle range) {
        return range.intersects(this.toRectangle());
    }

    /**
     * returns the portion of the image contained within the viewport
     * @param viewport
     * @return the portion of the whole image contained within viewport. Returns null if the image is not contained
     * within the viewport.
     */
    public Mat getSegmentContainedWithin (Rectangle viewport, Rectangle destRect) {
        Rectangle imageRect = this.toRectangle();
        Rectangle roi = viewport.intersection(imageRect);

        if (!roi.isEmpty()) {
            destRect.setBounds(roi);
            destRect.setLocation(roi.x - viewport.x, roi.y - viewport.y);
            // re-express the roi such that (0,0) is the image segments top left corner.
            roi.setLocation(roi.x - imageRect.x, roi.y - imageRect.y);

            Mat m = this.toMat();
            return m.submat(roi.y, roi.y + roi.height, roi.x, roi.x + roi.width);
        }

        return null;
    }

    private Rectangle toRectangle() {
        Mat mat = toMat();
        return new Rectangle(x,y,mat.width(),mat.height());
    }
}
