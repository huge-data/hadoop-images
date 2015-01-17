package zx.soft.hdfs.images.stegnography;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class Embed {

	public static final int[] message = { 1, 0, 1, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
			1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
	static double MSE;

	public static class EmbedBitMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

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
			String eachRow = value.toString();
			String fileName = ((FileSplit) reporter.getInputSplit()).getPath().getName();
			String lineNo = eachRow.substring(0, eachRow.indexOf("\t")).trim();
			String rowValue = eachRow.substring(eachRow.indexOf("\t") + 1);
			int nextLineNo = Integer.parseInt(lineNo) + 1;
			output.collect(new Text(fileName + "~" + lineNo), new Text("1 " + rowValue));
			output.collect(new Text(fileName + "~" + nextLineNo), new Text("0 " + rowValue));
		}
	}

	// Reducer Class

	public static class EmbedBitReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		/**
		 * reduce
		 * @param key : Text
		 * @param values : Iterator<Text>
		 * @param output : OutputCollector<Text, Text>
		 * @param reporter : Reporter
		 * @return void
		 * Reducer function
		 * 
		 * */

		String[][] rowData = null;

		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String[] rowSplit = null;
			String[] rowNum = key.toString().split("~");
			String rowString = null;
			int count = 0, imessageBit = 0;
			while (values.hasNext()) {
				count++;
				rowString = values.next().toString();
				rowSplit = rowString.split(" ");
				if (rowData == null) {
					rowData = new String[2][rowSplit.length - 1];
				}
				if (Integer.parseInt(rowSplit[0]) == 1) {
					imessageBit = Integer.parseInt(rowSplit[1]);
				}
				rowData[Integer.parseInt(rowSplit[0])] = Arrays.copyOfRange(rowSplit, 2, rowSplit.length);
			}
			try {
				if (count == 1) {
					if (rowData[0][0] == null && rowData[1][0] != null) {
						output.collect(new Text(key.toString()), new Text(writeArrayToString(rowData[1])));
					}
				} else if (count == 2) {
					if (Integer.parseInt(rowNum[1]) % 2 == 0) {

						calculateBits(rowData, 1, imessageBit);
						output.collect(new Text(key.toString()), new Text(writeArrayToString(rowData[1])));
					} else {
						calculateBits(rowData, 2, imessageBit);
						output.collect(new Text(key.toString()), new Text(writeArrayToString(rowData[1])));
					}

				}
			} catch (Exception e) {
				System.out.println("exception in reduce" + e);
			}

		}

		public String writeArrayToString(String[] arr) {
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < arr.length; i++) {
				sb.append(arr[i] + " ");
			}
			return sb.toString().trim();
		}

		public void calculateBits(String[][] rowData, int startBit, int messagebit) throws Exception {
			int d;
			int p1;
			int p2;
			int oldBit, newBit, bits;
			try {
				if (rowData.length != 2) {
					throw new Exception("Exception in calculate Bits length problem ");
				}
				for (int i = startBit; i < rowData[1].length; i = i + 2) {
					p1 = Integer.parseInt(rowData[1][i - 1]);
					p2 = Integer.parseInt(rowData[0][i]);

					d = Math.abs(p1 - p2);
					if (d < 3) {
						oldBit = Integer.parseInt(rowData[1][i]);
						newBit = embed(oldBit, 1, messagebit);
						rowData[1][i] = newBit + "";
						messagebit = messagebit + 1;
						MSE = MSE + Math.pow((oldBit - newBit), 2);

					} else if (d % 2 == 1) {
						bits = (int) Math.floor(Math.log10(d) / 0.3010);
						oldBit = Integer.parseInt(rowData[1][i]);
						newBit = embed(oldBit, bits, messagebit);
						rowData[1][i] = newBit + "";
						messagebit = messagebit + bits;
						MSE = MSE + Math.pow((oldBit - newBit), 2);
					} else {
						bits = (int) (Math.floor(Math.log10(d) / 0.3010) - 1);
						oldBit = Integer.parseInt(rowData[1][i]);
						newBit = embed(oldBit, bits, messagebit);
						rowData[1][i] = newBit + "";
						messagebit = messagebit + bits;
						MSE = MSE + Math.pow((oldBit - newBit), 2);
					}
				}
			} catch (Exception e) {
				throw new Exception("Exception in calculateBits --- " + e);
			}

		}

		public int embed(int iBit, int NumOfbits, int startbit) {
			String[] imageBit = Integer.toBinaryString(iBit).toString().split("");
			imageBit = Arrays.copyOfRange(imageBit, 1, imageBit.length);

			int j = 0;
			String sNewvalue = "";
			int iNewValue = 0;

			if ((startbit + NumOfbits) > message.length) {
				NumOfbits = message.length - startbit;
				if (NumOfbits <= 0) {
					return iBit;
				}
			}
			String[] newImagebit = new String[NumOfbits];
			int difference = imageBit.length - NumOfbits;
			for (int i = 0; i < imageBit.length; i++) {
				if (difference > 0) {
					difference--;
				} else {
					newImagebit[j] = message[startbit] + "";
					startbit++;
					j++;
				}
			}
			for (int i = 0; i < newImagebit.length; i++) {
				sNewvalue = sNewvalue + newImagebit[i];
			}
			int t = Integer.parseInt(sNewvalue, 2) - iBit % ((int) Math.pow(2, NumOfbits));

			if (t >= -(int) Math.floor((Math.pow(2, NumOfbits) - 1) / 2)
					&& t <= (int) Math.floor((Math.pow(2, NumOfbits) - 1) / 2)) {
				iNewValue = iBit + t;
			} else if (t >= -(int) Math.pow(2, NumOfbits) + 1
					&& t < -(int) Math.floor((Math.pow(2, NumOfbits) - 1) / 2)) {
				iNewValue = iBit + t + (int) Math.pow(2, NumOfbits);
			} else if (t >= (int) (Math.pow(2, NumOfbits) - 1) / 2 && t < (int) Math.pow(2, NumOfbits)) {
				iNewValue = iBit + t - (int) Math.pow(2, NumOfbits);
			}
			return iNewValue;

		}
	}

	public static class outputClass extends MultipleTextOutputFormat<Text, Text> {
		@Override
		protected String generateFileNameForKeyValue(Text key, Text value, String fileName) {
			String file = key.toString().split("~")[0];
			return file;
		}

		@Override
		protected Text generateActualKey(Text key, Text value) {
			key = new Text(key.toString().split("~")[1]);
			return key;
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Embed.class);
		conf.setJobName("Steg");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(EmbedBitMap.class);
		conf.setReducerClass(EmbedBitReduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(outputClass.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[0] + "/inter"));
		JobClient.runJob(conf);
		System.out.println(MSE);
	}

}
