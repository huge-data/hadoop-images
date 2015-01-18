package zx.soft.hdfs.images.mosaic;

import java.awt.*;
import java.io.IOException;

/**
 * Created by roinir on 18/06/2014.
 */
public class PyramidBinTile {

    private final ImagePyramidBin bin;
    private final String name;

    private int x;
    private int y;
    private int width;
    private int height;

    public PyramidBinTile(ImagePyramidBin bin, String name) {
        this.bin = bin;
        this.name = name;
    }

    public String getName(){
        return name;
    }

    public void setLocationInBin(int x, int y) throws IOException{

        if (x > bin.getImageWidth() || y > bin.getImageHeight()) throw new IOException("Tile location not in bin.");

        this.x = x;
        this.y = y;
    }

    public int getXInBin() {
        return x < 0? 0:x;
    }

    public int getYInBin() {
        return y < 0? 0:y;
    }

    public void setScaledDims(int scaledWidth, int scaledHeight) {
        this.width = scaledWidth;
        this.height = scaledHeight;
    }

    public int getScaledWidth(){ return width; }

    public int getScaledHeight() { return height; }

    public int getRawWidth() { return width*bin.getScale(); }

    public int getRawHeight() { return height*bin.getScale(); }

    public int getTileXInBin() {
        return x < 0 ? -x : 0;
    }

    public int getTileYInBin() {
        return y < 0 ? -y : 0;
    }

    public int getTileWidthInBin() {
        Rectangle binrect = new Rectangle(0,0,bin.getImageWidth(),bin.getImageHeight());
        Rectangle tilerect = new Rectangle(x,y,getScaledWidth(),getScaledHeight());
        return binrect.intersection(tilerect).width;

    }

    public int getTileHeightInBin() {
        Rectangle binrect = new Rectangle(0,0,bin.getImageWidth(),bin.getImageHeight());
        Rectangle tilerect = new Rectangle(x,y,getScaledWidth(),getScaledHeight());
        return binrect.intersection(tilerect).height;
    }

    public String toString(){
        return String.format("{%d,%d,%d,%d,%d,%d,%d,%d,%d}",
                bin.getScale(), bin.getRow(), bin.getCol(),
                bin.getImageWidth(), bin.getImageHeight(),
                x,y,width,height);
    }

    public static PyramidBinTile fromString(String str, String name) throws IOException {
        int [] paras = parseString(str, ",");

        int scale = paras[0];
        int row = paras[1];
        int col = paras[2];
        int binWidth = paras[3];
        int binHeight = paras[4];
        int x = paras[5];
        int y = paras[6];
        int width = paras[7];
        int height = paras[8];

        ImagePyramidBin bin = new ImagePyramidBin(scale,row,col);
        bin.setImageArea(binWidth,binHeight);

        PyramidBinTile tile = new PyramidBinTile(bin, name);
        tile.setLocationInBin(x,y);
        tile.setScaledDims(width,height);

        return tile;
    }

    private static int [] parseString(String str, String delim) throws IOException {
        str = str.trim();
        if (str.startsWith("{")) str = str.substring(1);
        if (str.endsWith("}")) str = str.substring(0,str.length()-1);

        String [] strArray = str.split(delim);
        if (strArray.length != 9) throw new IOException("Not enough input parameters in string. Expecting: 9 received: "+strArray.length);

        int [] parsed = new int [strArray.length];

        for (int i = 0; i < strArray.length; i++ ) {
            String para = strArray[i];
            try{
                parsed[i] = Integer.valueOf(para);
            } catch (NumberFormatException e){
                throw new IOException("Tried to convert: "+para+" to int");
            }
        }

        return parsed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PyramidBinTile that = (PyramidBinTile) o;

        if (height != that.height) return false;
        if (width != that.width) return false;
        if (x != that.x) return false;
        if (y != that.y) return false;
        if (!name.equals(that.name)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + x;
        result = 31 * result + y;
        result = 31 * result + width;
        result = 31 * result + height;
        return result;
    }

    public ImagePyramidBin getBin() { return bin; }
}
