package zx.soft.hdfs.images.mosaic;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class ImageTilesRecordReader implements RecordReader<LongWritable,MatPyramidWritable> {

    private CombineFileSplit fileSplit;
    int numRecords;
    int currentRecord = 0;
    FileSystem fs;
    private HashMap<String, String> configs = new HashMap<String, String>();

    ImageTilesRecordReader(CombineFileSplit fileSplit, JobConf job) throws IOException{
        this.fileSplit = fileSplit;
        this.numRecords = fileSplit.getNumPaths();
        this.fs = FileSystem.get(job);

        loadConfig(Utils.getPathToCachedFile(job, new Path(HadoopMosaicScaler.HDFS_PYRAMID_FILE).getName()));
    }


    private void loadConfig(Path cachePath) throws IOException {
        // note use of regular java.io methods here - this is a local file now
        BufferedReader wordReader = new BufferedReader(new FileReader(cachePath.toString()));
        final String delimiter = ":";
        try {
            String line;
            while ((line = wordReader.readLine()) != null) {
                String [] data = line.split(delimiter);
                configs.put(data[0],data[1]);
            }
        } finally {
            wordReader.close();
        }
    }

    @Override
    public boolean next(LongWritable key, MatPyramidWritable value) throws IOException {
        if(value == null){
            throw new IOException("value is null");
        }

        if (numRecords <= 0) {
            throw new IOException("no input record found");
        }

        // if have reached end of split
        if (currentRecord == numRecords) {
            return false;
        }

        // path of current record
        Path recordPath = fileSplit.getPath(currentRecord);

        String fileName = recordPath.getName();

        byte[] contents = new byte[(int) fileSplit.getLength(currentRecord)];
        FSDataInputStream in = null;
        try {
            in = fs.open(recordPath);
            IOUtils.readFully(in, contents, 0, contents.length);
            value.setMat(contents);
            String config = configs.get(fileName);
            if(config == null){
                throw new IOException("config is null for filename: "+fileName);
            }
            value.setConfig(config);

        } finally {
            IOUtils.closeStream(in);
        }

        currentRecord++;
        return true;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public MatPyramidWritable createValue() {
        return new MatPyramidWritable();
    }

    @Override
    public long getPos() throws IOException {
        return currentRecord;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public float getProgress() throws IOException {
        return (float) currentRecord / (float) numRecords;
    }
}
