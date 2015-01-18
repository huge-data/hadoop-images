package zx.soft.hdfs.images.mosaic;

import org.apache.hadoop.io.Text;
import org.opencv.core.Mat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MatPyramidWritable extends MatWritable{

    private Text pyramidConfig = new Text();

    public MatPyramidWritable(){
        super();
    }

    public MatPyramidWritable(Mat mat, String format, String configs) throws IOException{
        super(mat,format);
        pyramidConfig = new Text(configs);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);
        pyramidConfig.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
        pyramidConfig.readFields(dataInput);
    }


    public String getConfig(){
        return pyramidConfig.toString();
    }

    public void setConfig(String configs){
        pyramidConfig = new Text(configs);
    }
}
