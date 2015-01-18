package zx.soft.hdfs.images.mosaic;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopMosaicScaler extends Configured implements Tool {

	public static final String JOB_NAME = "hadoopMosaicScaler";

	public static final String CONF_IMAGE_FORMAT = "hadoopmosaicscaler.conf.IMAGE_FORMAT";

	public static final String HDFS_PYRAMID_FILE = "pyramid.conf";
	public static final String OPENCV_LIB = "libopencv_java246.so";

	private boolean generateConfig = false;
	private final PyramidConfigBuilder.PyramidParams pyramidParams = new PyramidConfigBuilder.PyramidParams();

	private String pathToPyramidFile;
	private String pathToTiles;
	private String pathToResult;
	private String openCvLib;
	private String imageExt = "jpg";

	@Override
	public int run(String[] args) throws Exception {
		parseInput(args);

		if (generateConfig)
			PyramidConfigBuilder.start(pyramidParams);

		Job job = Job.getInstance(getConf());

		Utils.cacheLocalFile(job, pathToPyramidFile, HDFS_PYRAMID_FILE);
		Utils.cacheLocalFile(job, openCvLib, OPENCV_LIB);

		JobConf conf = new JobConf(job.getConfiguration(), HadoopMosaicScaler.class);

		conf.setJobName(JOB_NAME);
		conf.setStrings(CONF_IMAGE_FORMAT, imageExt);

		conf.setMapperClass(TileLoadMap.class);
		conf.setReducerClass(TileAssemblerReduce.class);

		conf.setPartitionerClass(HashPartitioner.class);

		conf.setMapOutputValueClass(MatTileWritable.class);
		conf.setMapOutputKeyClass(Text.class);

		conf.setInputFormat(ImageTilesInputFormat.class);
		conf.setOutputFormat(MultipleMatsOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(pathToTiles));
		FileOutputFormat.setOutputPath(conf, new Path(pathToResult));

		JobClient.runJob(conf);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new HadoopMosaicScaler(), args);
		System.exit(res);
	}

	private void parseInput(String[] args) {
		pathToPyramidFile = pyramidParams.outputPath;
		ArrayList<String> otherArgs = new ArrayList<String>();

		for (int i = 0; i < args.length - 1; i++) {
			if (args[i].startsWith("-")) {
				String key = args[i].substring(1);
				String val = args[i + 1];
				i++;

				if (key.equals("imageFormat")) {
					imageExt = val;
				} else if (key.equals("config")) {
					pathToPyramidFile = val;
					pyramidParams.outputPath = val;
				} else if (key.equals("positions")) {
					generateConfig = true;
					pyramidParams.pathToPositions = val;
				} else if (key.equals("outdir")) {
					pathToResult = val;
				} else if (key.equals("opencv")) {
					openCvLib = val;
				} else if (key.equals("tilesDir")) {
					pathToTiles = val;
				} else if (key.equals("tilesSize")) {
					pyramidParams.tileWidth = Integer.valueOf(val.split(",")[0]);
					pyramidParams.tileHeight = Integer.valueOf(val.split(",")[1]);
				} else if (key.equals("scalingStep")) {
					pyramidParams.scallingStep = Integer.valueOf(val);
				} else if (key.equals("scaleBounds")) {
					pyramidParams.minScale = Integer.valueOf(val.split(",")[0]);
					pyramidParams.maxScale = Integer.valueOf(val.split(",")[1]);
				} else if (key.equals("centered")) {
					pyramidParams.centered = Boolean.valueOf(val);
				}

			} else {
				otherArgs.add(args[i]);
			}
		}
	}

}
