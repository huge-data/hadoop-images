package zx.soft.hdfs.images.mosaic;

import java.awt.*;
import java.io.*;
import java.util.ArrayList;

/**
 * Created by roinir on 17/06/2014.
 */
public class ImagePyramidBuilder {
    private int minY = 0;
    private int maxY = 0;
    private int minX = 0;
    private int maxX = 0;
    private int meanTileWidth = 0;
    private int meanTileHeight = 0;
    private int maxTileWidth = 0;
    private int maxTileHeight = 0;
    // settings parameters
    private int scallingStep;
    private int portWidth;
    private int portHeight;
    private boolean centered = true;
    private int mMinScale = 1;
    private int mMaxScale = -1;

    private class ImageTile {
        public int x;
        public int y;
        public int width;
        public int height;
        public String name;

        private ImageTile(int x, int y, int width, int height, String name) {
            this.x = x;
            this.y = y;
            this.width = width;
            this.height = height;
            this.name = name;
        }

        public boolean inBin(ImagePyramidBin bin, int startX, int startY){
            int scale = bin.getScale();
            int binWidth = bin.getImageWidth();
            int binHeight = bin.getImageHeight();

            int X = (int) Math.floor(1.0*(startX + x - (bin.getCol()-1)*binWidth*scale)/scale);
            int Y = (int) Math.floor(1.0*(startY + y - (bin.getRow()-1)*binHeight*scale)/scale);

            Rectangle binArea =  new Rectangle(0,0,binWidth,binHeight);
            Rectangle tileArea = new Rectangle(X,Y,(int) Math.floor(1.0*width/scale),(int) Math.floor(1.0*height/scale));

            return binArea.intersects(tileArea);
        }
    }

    private ArrayList<ImageTile> tiles = new ArrayList<ImageTile>();

    public ImagePyramidBuilder(String positionsPath) throws IOException{
        parsePositions(positionsPath);
    }

    public ImagePyramidBuilder(String positionsPath, int viewportWidth, int viewportHeight, int scallingStep, boolean centered) throws IOException{
        this.centered = centered;
        this.portHeight = viewportHeight;
        this.portWidth = viewportWidth;
        this.scallingStep = scallingStep;
        parsePositions(positionsPath);
    }

    public ImagePyramid build() throws IOException{
        // calculate the best viewport if the user hasn't specified it
        if(portHeight <= 0 && portWidth <= 0) autoSettings();

        System.out.printf("viewport height: %d width: %d, minX: %d, maxX: %d\n",portHeight,portWidth,minX,maxX);

        ArrayList<ImagePyramidBin> bins = new ArrayList<ImagePyramidBin>();
        int minScale = mMinScale;
        int maxScale = minScale;
        while (maxScale * portWidth < maxX - minX || maxScale * portHeight < maxY - minY) maxScale *= scallingStep;

        int startX = centered ? ( maxScale*portWidth - (maxX - minX) ) / 2 - minX : - minX;
        int startY = centered ? ( maxScale*portHeight - (maxY - minY) ) / 2 - minY: - minY;

        int stopScale = mMaxScale == -1? maxScale:mMaxScale;

        for (int scale = minScale; scale <= stopScale; scale *= scallingStep) {

            int maxRows = (int) Math.ceil(1.0*maxScale/scale);
            for (int row = 1; row <= maxRows; row++) {

                int maxCols = (int) Math.ceil(1.0*maxScale/scale);
                for(int col = 1; col <= maxCols; col++){

                    ImagePyramidBin bin = new ImagePyramidBin(scale,row,col);
                    bin.setImageArea(portWidth,portHeight);
                    bins.add(bin);

                    System.out.printf("scale: %d, row: %d, col: %d, portWidth: %d, portHeight: %d\n",scale,row,col,portWidth,portHeight);

                    for (ImageTile tile : tiles) {
                        if(tile.inBin(bin, startX, startY)){
                            PyramidBinTile binTile = bin.addTile(tile.name);
                            int x = (int) Math.floor(1.0*(startX + tile.x - (col-1)*portWidth*scale)/scale);
                            int y = (int) Math.floor(1.0*(startY + tile.y - (row-1)*portHeight*scale)/scale);
                            //System.out.printf("x: %d, y: %d\n",x,y);
                            binTile.setLocationInBin(x,y);
                            binTile.setScaledDims((int) Math.floor(1.0*tile.width/scale),(int) Math.floor(1.0*tile.height/scale));
                        }
                    }
                }
            }
        }

        return new ImagePyramid(bins);
    }

    public void autoSettings(){
        portWidth = meanTileWidth;
        portHeight = meanTileHeight;
        scallingStep = 2;
    }

    private void parsePositions(String filePath) throws IOException{
        DataInputStream in = null;
        try {
            FileInputStream fstream = new FileInputStream(filePath);
            in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line;
            String delim = ",";
            while ((line = br.readLine()) != null)   {
                // Print the content on the console
                String [] data = line.split(delim);
                addTile(data[0],Integer.valueOf(data[1]),Integer.valueOf(data[2]),Integer.valueOf(data[3]),Integer.valueOf(data[4]));
            }
        }finally {
            if( in != null ) in.close();
        }
    }

    private void addTile(String name, int x, int y, int height, int width){
        if(x + width > maxX) maxX = x + width;
        if(x < minX) minX = x;
        if(y + height > maxY) maxY = y + height;
        if(y < minY) minY = y;

        tiles.add(new ImageTile(x,y,width,height,name));

        meanTileHeight = (meanTileHeight*(tiles.size() - 1) + height)/tiles.size();
        meanTileWidth = (meanTileWidth*(tiles.size() - 1) + width)/tiles.size();

        if(maxTileHeight < height ) maxTileHeight = height;
        if(maxTileWidth < width ) maxTileWidth = width;
    }

    public void setScaleRange(int min, int max) throws IOException{
        if (min > max && max != -1) throw new IOException("Min scale greater then max scale. minScale: "+min+", maxScale: "+max);
        if (min < 1) throw new IOException("Min must be a positive number greater or equal to 1. minScale: "+min);
        if (max < -1) throw new IOException("Max must be a positive number greater or equal to 1 (or -1 for auto). maxScale: "+max);
        mMaxScale = max;
        mMinScale = min;
    }
}
