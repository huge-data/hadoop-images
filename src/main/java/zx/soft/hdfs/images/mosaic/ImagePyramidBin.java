package zx.soft.hdfs.images.mosaic;

import java.util.ArrayList;

public class ImagePyramidBin {

    private int scale;
    private int row;
    private int col;

    private int width;
    private int height;

    private ArrayList<String> names = new ArrayList<String>();
    private ArrayList<PyramidBinTile> tiles = new ArrayList<PyramidBinTile>();

    public PyramidBinTile addTile(String name) {
        PyramidBinTile tile = new PyramidBinTile(this, name);
        tiles.add(tile);
        names.add(name);
        return tile;
    }

    public PyramidBinTile getTile(String name) {
        int i = names.indexOf(name);

        if(i != -1) return tiles.get(i);
        else return null;
    }

    public  ArrayList<String> getNames() {
        return names;
    }

    public ImagePyramidBin(int scale, int row, int col) {
        this.scale = scale;
        this.row = row;
        this.col = col;
    }

    public int getImageWidth() { return width; }

    public int getImageHeight() { return height; }

    public void setImageArea(int width, int height){
        this.width = width;
        this.height = height;
    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }

    public int getRow() {
        return row;
    }

    public void setRow(int row) {
        this.row = row;
    }

    public int getCol() {
        return col;
    }

    public void setCol(int col) {
        this.col = col;
    }



}
