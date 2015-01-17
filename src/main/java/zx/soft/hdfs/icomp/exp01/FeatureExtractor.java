package zx.soft.hdfs.icomp.exp01;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FeatureExtractor {

	public static void main(String[] args) throws Exception {
		int resultado = ToolRunner.run(new FeatureExtractorRunner(), args);
		System.exit(resultado);
	}

}

class FeatureExtractorRunner extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Parâmetros: dir-entrada dir-saida");
			return -1;
		}

		String entrada = args[0];
		String saida = args[1];

		long inicio = System.currentTimeMillis();
		executarJob(entrada, saida);
		long fim = System.currentTimeMillis();
		System.out.println("Tempo total: " + ((fim - inicio) / 1000) + "s");
		return 0;
	}

	private void executarJob(String entrada, String saida) throws IOException, InterruptedException,
			ClassNotFoundException {

		JobConf configuracao = new JobConf();

		FileInputFormat.addInputPath(configuracao, new Path(entrada));
		FileOutputFormat.setOutputPath(configuracao, new Path(saida));

		Job job = new Job(configuracao, "featureExtractor");
		job.setMapperClass(FeatureExtractorMapper.class);
		job.setReducerClass(FeatureExtractorReducer.class);
		job.setJarByClass(FeatureExtractor.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
	}

}
