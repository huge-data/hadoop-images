package zx.soft.hdfs.icomp.io;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 * Splits a collection of potentialy huge images in slightly smaller ones (1/4 of original size).
 * 
 * @author luiz
 */
public class ImageSplitter {

	static Configuration confHadoop = new Configuration();

	public static void main(String args[]) throws Exception {
		if (args.length != 2) {
			System.err.println("argumentos: dir-de-entrada arquivo-de-saida");
			System.exit(1);
		}

		FileSystem fs = FileSystem.get(confHadoop);
		Path inPath = new Path(args[0]);
		Path outPath = new Path(args[1] + "/dataset");
		FSDataInputStream in = null;
		SequenceFile.Writer writer = null;
		List<Path> files = listFiles(inPath, jpegFilter);
		try {
			writer = SequenceFile.createWriter(fs, confHadoop, outPath, Text.class, BytesWritable.class);
			for (Path p : files) {
				in = fs.open(p);
				BufferedImage originalImage = ImageIO.read(in);
				BufferedImage[][] chunks = ImageUtils.splitImage(originalImage, 2, 2);
				for (int i = 0; i < chunks.length; i++) {
					for (int j = 0; j < chunks[i].length; j++) {
						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						ImageIO.write(chunks[i][j], "jpg", baos);
						writer.append(new Text(p.getName() + "_" + i + "_" + j), new BytesWritable(baos.toByteArray()));
					}
				}
				in.close();
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}

	private static List<Path> listFiles(Path path, PathFilter filter) throws IOException {
		ArrayList<Path> files = new ArrayList<Path>();
		FileSystem fs = FileSystem.get(confHadoop);
		FileStatus[] status = fs.listStatus(path);
		for (int i = 0; i < status.length; i++) {
			Path p = status[i].getPath();
			if (filter.accept(p)) {
				files.add(p);
			}
		}
		return files;
	}

	private static PathFilter jpegFilter = new PathFilter() {
		@Override
		public boolean accept(Path path) {
			return path.getName().endsWith("jpg");
		}
	};

}
