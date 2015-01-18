package zx.soft.hdfs.images.mosaic;

import java.io.IOException;

public class PyramidConfigBuilder {

    public static class PyramidParams{
        public int tileWidth = 600;
        public int tileHeight = 600;
        public int scallingStep = 2;
        public int maxScale = -1;
        public int minScale = 1;
        public boolean centered = true;
        public String pathToPositions = "positions.csv";
        public String outputPath = "pyramid.conf";
    }

    public static void main(String [] args) throws IOException{
        PyramidParams para = new PyramidParams();
        para.maxScale = 8;
        para.pathToPositions = "/Users/roinir/Projects/HadoopMosaicScaler/runnable/positions.csv";
        start(para);
    }

    public static void start(PyramidParams params) throws IOException{

        ImagePyramidBuilder builder = new ImagePyramidBuilder(params.pathToPositions,params.tileWidth,params.tileHeight,params.scallingStep,params.centered);
        builder.setScaleRange(params.minScale,params.maxScale);
        ImagePyramid pyramid = builder.build();
        pyramid.toMapFile(params.outputPath);

    }
}
