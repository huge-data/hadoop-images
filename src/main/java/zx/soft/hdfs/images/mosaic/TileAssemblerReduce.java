package zx.soft.hdfs.images.mosaic;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.opencv.core.Mat;
import org.opencv.core.Scalar;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;


public class TileAssemblerReduce extends MapReduceBase implements Reducer<Text,MatTileWritable,Text,MatWritable>{

    private String format;

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        format = job.getStrings(HadoopMosaicScaler.CONF_IMAGE_FORMAT)[0];

        try {
            File libFile = new File(Utils.getPathToCachedFile(job, new Path(HadoopMosaicScaler.OPENCV_LIB).getName()).toString());
            System.load(libFile.getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        } catch (UnsatisfiedLinkError e){
            throw e;
        }

    }

    @Override
    public void reduce(Text key, Iterator<MatTileWritable> matTiles, OutputCollector<Text, MatWritable> output, Reporter reporter) throws IOException {
        Mat image = null;
//        int [] parsedKey = (int []) key.get();

        while (matTiles.hasNext()) {

            MatTileWritable matTileWritable = matTiles.next();
            Mat tile = matTileWritable.toMat();

            int x = matTileWritable.getX();
            int y = matTileWritable.getY();
            int width = tile.width();
            int height = tile.height();

            if (image == null) image = new Mat(matTileWritable.getParentHeight(),matTileWritable.getParentWidth(), tile.type(), new Scalar(0));

            Mat subMat = tile.submat(0,y+height > image.rows() ? image.rows()-y:height,0,x+width > image.cols() ? image.cols() - x:width);
            subMat.copyTo(image.submat(y, y+subMat.rows(), x, x+subMat.cols()));

            subMat.release();
            tile.release();

        }
        Text outKey = key;

        output.collect(outKey,new MatWritable(image,format));
    }
}
