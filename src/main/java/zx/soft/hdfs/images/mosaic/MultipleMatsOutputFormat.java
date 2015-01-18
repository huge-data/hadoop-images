package zx.soft.hdfs.images.mosaic;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.DataOutputStream;
import java.io.IOException;

public class MultipleMatsOutputFormat extends MultipleOutputFormat <Text,MatWritable> {

    protected static class MatRecordWriter implements RecordWriter<Text,MatWritable> {

        private DataOutputStream out;

        public MatRecordWriter(DataOutputStream out) {
            this.out = out;
        }

        @Override
        public synchronized void write(Text text, MatWritable matWritable) throws IOException {
            out.write(matWritable.toByte());
        }

        @Override
        public synchronized void close(Reporter reporter) throws IOException {
            out.close();
        }
    }

    @Override
    protected String generateFileNameForKeyValue(Text key, MatWritable value, String name) {
        return key.toString();
    }

    @Override
    protected RecordWriter<Text, MatWritable> getBaseRecordWriter(FileSystem fs, JobConf job, String name, Progressable progress) throws IOException {
        Path file = FileOutputFormat.getTaskOutputPath(job, name);
        FSDataOutputStream fileOut = fs.create(file, progress);
        return new MatRecordWriter(fileOut);
    }
}
