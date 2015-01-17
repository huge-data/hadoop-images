package zx.soft.hdfs.images.stegnography;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Iterator;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ImagePreProcessor extends Configured implements Tool {

	/**
	 * Function to read the image files and convert them to integer files
	 * @param path
	 * @param conf
	 * @param outpath
	 * @throws Exception
	 */
	public static String readWriteFile(Path path, JobConf conf) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		//		FSDataOutputStream out;
		//FSDataOutputStream log = fs.create(new Path(outpath.toString()+"/logFile.txt"));
		//		FileStatus[] status = fs.listStatus(path);
		//		int sequence =0;
		int i;
		//        for (int i=0;i<status.length;i++){

		StringBuffer sb = new StringBuffer();

		BufferedImage image = ImageIO.read(fs.open(path));
		//		out = fs.create(new Path(outpath.toString()+"\\"+path.getName()+".txt"));
		System.out.println("path.getName() -- " + path.getName());
		//		System.out.println("outpath.toString() -- "+outpath.toString());
		int value, red, blue, green;
		int imgHeight = image.getHeight();
		int[][] aValue = new int[image.getHeight() * 3][image.getWidth()];
		for (i = 0; i < image.getHeight(); i++) {

			for (int j = 0; j < image.getWidth(); j++) {
				value = image.getRGB(i, j);
				red = (value >> 16) & 0xff;
				green = (value >> 8) & 0xff;
				blue = value & 0xff;
				aValue[i][j] = red;
				aValue[imgHeight + i][j] = green;
				aValue[(2 * imgHeight) + i][j] = blue;
			}
		}
		for (i = 0; i < aValue.length; i++) {
			sb.append((i % image.getHeight() + 1) + "\t");
			for (int j = 0; j < aValue[i].length; j++) {
				sb.append("" + aValue[i][j] + " ");
			}
			sb.append("\n");
		}
		//out.close();
		//        }
		return sb.toString();
	}

	/***
	 * Function to copy the image files from local file system to hdfs
	 * @param conf
	 * @param sourcePath
	 * @param destinationPath
	 * @throws Exception
	 */
	public static void copyFromLocal(JobConf conf, String sourcePath, String destinationPath) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		Path interPath = new Path(destinationPath);
		fs.mkdirs(interPath);
		fs.copyFromLocalFile(new Path(sourcePath), interPath);
	}

	/**
	 * Method to convert the Image Files to Integer Text Files
	 * @param conf
	 * @param sourcePath
	 * @param destinationFolder
	 * @throws Exception
	 */
	public static void convertImagesToIntegerFiles(JobConf conf, String sourcePath, String destinationFolder)
			throws Exception {
		readWriteFile(new Path(sourcePath), conf);
	}

	/**
	 * 
	 * @param conf
	 * @param path
	 * @param outPath
	 * Folder where the output files are required
	 * @return
	 * @throws Exception
	 */
	public static Path writeImageNamesToFile(JobConf conf, Path path, Path outPath) throws Exception {
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] status = fs.listStatus(path);
		String stringPath;
		FSDataOutputStream out = fs.create(new Path(outPath.toString() + "\\out.txt"));
		for (int i = 0; i < status.length; i++) {
			System.out.println("Path " + FileSystem.get(conf).getWorkingDirectory());
			stringPath = status[i].getPath().getName();
			out.writeChars(stringPath + "\n");
		}
		return new Path(outPath.toString() + "\\out.txt");
	}

	//	public static void log(JobConf conf, String str) throws IOException  {
	//		FileSystem fs = FileSystem.get(conf);
	//		FSDataOutputStream out = fs.create(new Path("\\user\\cloudera\\project\\imageLogFiles\\log"),false);
//		out.writeChars(str);
	//		out.close();
	//	}

	public static class ImagePreprocessorMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		public JobConf conf;

		/**
		 * map
		 * @param key : LongWritable
		 * @param value : Text
		 * @param output : OutputCollector<Text, Text>
		 * @param reporter : Reporter
		 * @return void
		 * Mapper function
		 * 
		 */
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String strPath = value.toString();
			System.out.println("file name-----" + strPath);
			//			FileSystem fs = FileSystem.get(conf);
			String imageFilesName = conf.getStrings("working.directory").toString();
			//			Path currentPath = imageFilesName;
			Path path = new Path(imageFilesName + "\\" + strPath);
			//String outputString="";
			System.out.println("path in mapper-------" + path);
			//			Path outPath = new Path(imageFilesName+"\\intImages");
			//			FileSystemUtil.log(conf, path.toString());
			//			FileSystemUtil.log(conf, outPath.toString());
			try {
				String intFile = readWriteFile(path, conf);
				output.collect(new Text(strPath), new Text(intFile));
			} catch (Exception e) {
				throw new IOException("Exception in Preprocessor mapper" + e);
			}
		}

		@Override
		public void configure(JobConf arg0) {
			// TODO Auto-generated method stub
			//super.configure(arg0);
			System.out.println("in configure------" + arg0);
			conf = arg0;
		}
	}

	public static class ImagePreprocessorReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuilder sb = new StringBuilder();
			System.out.println("In reduce");
			while (values.hasNext()) {
				sb.append(values.next());
			}
			output.collect(key, new Text(sb.toString()));
		}

	}

	public static class outputClass extends MultipleTextOutputFormat<Text, Text> {
		@Override
		protected String generateFileNameForKeyValue(Text key, Text value, String fileName) {
			String file = key.toString();
			return file;
		}

		@Override
		protected Text generateActualKey(Text key, Text value) {
			key = new Text("");
			return key;
		}
	}

	public static void copyFilesFromFolder(JobConf conf, String path, Path outputPath) throws Exception {

		FileSystem fs = FileSystem.get(conf);
		File dir = new File(path);
		//System.out.println("input Path "+path.toString());
		FileFilter fileFilter = new FileFilter() {
			@Override
			public boolean accept(File file) {
				return !file.isDirectory();
			}
		};
		if (dir.isDirectory()) {
			File[] list = dir.listFiles(fileFilter);
			for (int i = 0; i < list.length; i++) {
				fs.copyFromLocalFile(new Path(list[i].getPath()), outputPath);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(ImagePreProcessor.class);
		conf.setJobName("preprocessor");

		String baseOutputPath = args[1];
		FileSystem fs = FileSystem.get(conf);
		Path interPath = new Path(baseOutputPath + "_inter");
		fs.mkdirs(interPath);
		Path imageFilesPath = new Path(interPath + "\\imageFiles");
		fs.mkdirs(imageFilesPath);
		System.out.println("imageFilesPath--------------->" + imageFilesPath);
		conf.setStrings("working.directory", imageFilesPath.toString());

		copyFilesFromFolder(conf, args[0], imageFilesPath);
		//		conf.setJobName("FileSystemMove");
		Path fileNamesPath = new Path(interPath + "\\fileNames");
		System.out.println("fileNamesPath------------>" + fileNamesPath);
		writeImageNamesToFile(conf, imageFilesPath, fileNamesPath);
		//		FileSystem.get(conf).setWorkingDirectory(imageFilesPath);
		//		Path finalIntegerPath = new Path(imageFilesPath.toString()+"\\intImages");
		//		fs.mkdirs(finalIntegerPath);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		System.out.println("setting mapper");
		conf.setMapperClass(ImagePreprocessorMap.class);
		conf.setReducerClass(ImagePreprocessorReduce.class);

		System.out.println("mapper set");
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(outputClass.class);

		FileInputFormat.setInputPaths(conf, fileNamesPath);
		FileOutputFormat.setOutputPath(conf, new Path(interPath.toString() + "\\temp"));
		JobClient.runJob(conf);
		return 0;
	}

	/***
	 * 
	 * @param args	
	 * args[0] - folder containing image files
	 * args[1] - output folder
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception {

		int res = ToolRunner.run(new Configuration(), new ImagePreProcessor(), args);
		System.exit(res);
	}

}
