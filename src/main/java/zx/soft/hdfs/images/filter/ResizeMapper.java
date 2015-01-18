package zx.soft.hdfs.images.filter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.core.MatOfRect;
import org.opencv.core.Point;
import org.opencv.core.Rect;
import org.opencv.core.Scalar;
import org.opencv.highgui.Highgui;
import org.opencv.objdetect.CascadeClassifier;
import org.xml.sax.SAXException;

public class ResizeMapper extends Mapper<Text, BytesWritable, NullWritable, NullWritable> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// http://docs.opencv.org/doc/tutorials/introduction/java_eclipse/java_eclipse.html#java-eclipse
		// 根据网址安装OpenCV
		System.loadLibrary(Core.NATIVE_LIBRARY_NAME);

	}

	@Override
	protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

		String tableName = "final_table";
		Configuration hbaseConf = HBaseConfiguration.create();

		Parser parser = new AutoDetectParser();
		BodyContentHandler handler = new BodyContentHandler(10000000);
		Metadata metadata = new Metadata();

		//mos.write(NullWritable.get(), value,filenameKey.toString() );
		byte[] bytes = value.getBytes();
		try {
			parser.parse(new ByteArrayInputStream(bytes), handler, metadata, new ParseContext());
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TikaException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println(metadata.get("User Comment"));
		//System.out.println("===============================");
		System.out.println("\nRunning DetectFaceDemo");

		// Create a face detector from the cascade file in the resources directory.
		CascadeClassifier faceDetector = new CascadeClassifier(getClass().getResource("/lbpcascade_frontalface.xml")
				.getPath());

		MatOfByte matOfByte = new MatOfByte(bytes);
		Mat mat = Highgui.imdecode(matOfByte, 1);

		// Detect faces in the images // MatOfRect is a special container class for Rect.
		MatOfRect faceDetections = new MatOfRect();
		faceDetector.detectMultiScale(mat, faceDetections);

		//Here is where I am seeing the error
		System.out.println(String.format("Detected %s faces", faceDetections.toArray().length));

		// Draw a bounding box around each face.
		for (Rect rect : faceDetections.toArray()) {
			Core.rectangle(mat, new Point(rect.x, rect.y), new Point(rect.x + rect.width, rect.y + rect.height),
					new Scalar(0, 255, 0));
		}

		MatOfByte matOfByteOut = new MatOfByte();

		// encoding to jpg, so that your image does not lose information like with jpeg.
		Highgui.imencode(".jpg", mat, matOfByteOut);

		byte[] return_buff = matOfByteOut.toArray();

		//Insert the return_buff byte[] in HBase table // //Create an instance of the Hbase table
		@SuppressWarnings("deprecation")
		HTable table = new HTable(hbaseConf, tableName);
		for (int i = 0; i < 10; i++) {
			//Create a put instance for a row with row key id and //Generate a unique ID for the image
			UUID id = UUID.randomUUID();
			Put put = new Put(Bytes.toBytes(id.toString()));
			put.add(Bytes.toBytes("image_metadata"), Bytes.toBytes("UserComment"),
					Bytes.toBytes(metadata.get("User Comment")));
			put.add(Bytes.toBytes("image_metadata"), Bytes.toBytes("Content-Type"),
					Bytes.toBytes(metadata.get("Content-Type")));
			put.add(Bytes.toBytes("image_metadata"), Bytes.toBytes("Image Width"),
					Bytes.toBytes(metadata.get("Exif Image Width")));
			put.add(Bytes.toBytes("image_metadata"), Bytes.toBytes("Image Height"),
					Bytes.toBytes(metadata.get("Exif Image Height")));
			put.add(Bytes.toBytes("detected_image"), Bytes.toBytes("image"), return_buff);
			table.put(put);
		}
		table.close();

	}

}
