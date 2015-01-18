package zx.soft.hdfs.images.mosaic;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

public class Utils {

	/** Loads OpenCV binary .so file from input directory*/
	public static void loadOpenCVBinary(String ocvpth) throws IOException {
		if (!ocvpth.endsWith("/"))
			ocvpth += "/";
		File ocvDir = new File(ocvpth);
		File[] files = ocvDir.listFiles(new FileFilter() {
			@Override
			public boolean accept(File arg) {
				if (arg.getName().startsWith("libopencv_java")
						&& (arg.getName().endsWith(".so") || arg.getName().endsWith(".dylib"))) {
					return true;
				} else {
					return false;
				}
			}
		});
		if (files == null || files.length == 0)
			throw new IOException("could not find valid OpenCV .so or .dylib file in " + ocvpth);
		//
		// Load OpenCV binary, assuming it's the first .so file in array
		try {
			System.load(files[0].getAbsolutePath());
		} catch (Exception e) {
			throw new IOException(e.getMessage());
		}
	}

	public static void cacheLocalFile(Job job, String localPath, String HDFSPath) throws IOException {
		Path hdfsPath = new Path(HDFSPath);

		FileSystem fs = FileSystem.get(job.getConfiguration());
		fs.copyFromLocalFile(false, true, new Path(localPath), hdfsPath);

		//		job.addCacheFile(hdfsPath.toUri());
	}

	public static Path getPathToCachedFile(JobConf conf, String fileName) throws IOException {
		Job job = Job.getInstance(conf);

		//		Path[] cacheFiles = job.getLocalCacheFiles();

		//		if (cacheFiles == null || cacheFiles.length == 0) {
		//			throw new IOException("Cache is empty.");
		//		}
		//
		//		for (Path cachedFile : cacheFiles) {
		//			//            Path cachedFile = new Path(cacheURI);
		//			if (cachedFile.getName().equals(fileName)) {
		//				return cachedFile;
		//			}
		//		}

		throw new IOException("File not found in cache.");
	}

}
