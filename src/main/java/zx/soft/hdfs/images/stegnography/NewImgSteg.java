package zx.soft.hdfs.images.stegnography;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NewImgSteg extends Configured implements Tool {

	public static int[] message;
	public static ArrayList<int[]> alMessage = new ArrayList<int[]>();
	//	static double MSE;
	public static HashMap<String, Double> hmMSE = new HashMap<String, Double>();

	public static String writeArrayToString(String[] arr) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < arr.length; i++) {
			sb.append(arr[i] + " ");
		}
		return sb.toString().trim();
	}

	// Step 1: Calculating the individual no of bits that can be embedded in each row

	// Mapper Class

	public static class BitCalcMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

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
			System.out.println("row------" + eachRow);
			System.out.println("index------" + eachRow.indexOf("\t"));
			String fileName = ((FileSplit) reporter.getInputSplit()).getPath().getName();
			String lineNo = eachRow.substring(0, eachRow.indexOf("\t")).trim();
			String rowValue = eachRow.substring(eachRow.indexOf("\t") + 1);
			int nextLineNo = Integer.parseInt(lineNo) + 1;
			output.collect(new Text(fileName + "~" + lineNo), new Text("1 " + rowValue));
			output.collect(new Text(fileName + "~" + nextLineNo), new Text("0 " + rowValue));
		}

	}

	// Reducer Class

	public static class BitCalcReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
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
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String[][] rowData = null;
			String[] rowSplit;
			String[] numOfBits = key.toString().split("~");
			String rowString = null;
			int count = 0;
			while (values.hasNext()) {
				count++;
				rowString = values.next().toString();
				rowSplit = rowString.split(" ");
				if (rowData == null) {
					rowData = new String[2][rowSplit.length - 1];
				}
				rowData[Integer.parseInt(rowSplit[0])] = Arrays.copyOfRange(rowSplit, 1, rowSplit.length);
			}
			try {
				if (count == 1) {
					if (rowData[0][0] == null && rowData[1][0] != null) {
						output.collect(new Text(key.toString()), new Text("0 " + writeArrayToString(rowData[1])));
					}
				} else if (count == 2) {
					if (Integer.parseInt(numOfBits[1]) % 2 == 0) {
						output.collect(new Text(key.toString()), new Text(calculateBits(rowData, 1) + " "
								+ writeArrayToString(rowData[1])));
					} else {
						output.collect(new Text(key.toString()), new Text(calculateBits(rowData, 2) + " "
								+ writeArrayToString(rowData[1])));
					}

				}
			} catch (Exception e) {
				System.out.println("exception in reduce" + e);
			}

		}

		/**
		 * 
		 * @param inputString - array of row strings
		 * @param startBit - bit in which the embedding starts
		 * @return - total number of bits that can be embedded on that row
		 * @throws Exception
		 */

		public int calculateBits(String[][] inputString, int startBit) throws Exception {
			int d;
			int p1;
			int p2;
			int totalbits = 0;
			try {
				if (inputString.length != 2) {
					throw new Exception("Exception in calculate Bits length problem ");
				}
				for (int i = startBit; i < inputString[1].length; i = i + 2) {
					p1 = Integer.parseInt(inputString[1][i - 1]);
					p2 = Integer.parseInt(inputString[0][i]);
					d = Math.abs(p1 - p2);
					if (d < 3) {
						totalbits = totalbits + 1;
					} else if (d % 2 == 1) {
						totalbits = totalbits + (int) Math.floor(Math.log10(d) / 0.3010);
					} else {
						totalbits = totalbits + (int) (Math.floor(Math.log10(d) / 0.3010) - 1);
					}
				}
			} catch (Exception e) {
				throw new Exception("Exception in calculateBits --- " + e);
			}
			return totalbits;
		}
	}

	//step 2 : performs the accumulate the number of bits that can be embedded in a row with previous other rows
	//Mapper class

	public static class AggregateMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		Text tRowNumber = new Text();
		Text tValue = new Text();

		/**
		 * @param key : LongWritable
		 * @param value : Text
		 * @param output : OutputCollector<Text, Text>
		 * @param reporter : Reporter
		 * @return void
		 * Mapper function
		 */
		@SuppressWarnings("unused")
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			int iCount = 0, iRowNumber = 0;
			String[] saValue = value.toString().split("\t");
			String sValue;
			String fileName = ((FileSplit) reporter.getInputSplit()).getPath().getName();
			String height = fileName.substring(fileName.indexOf("_") + 1);
			String width = height.substring(0, height.indexOf("x"));
			height = height.substring(height.indexOf("x") + 1, height.indexOf("."));

			try {
				int iHeight = Integer.parseInt(height);
				int iWidth = Integer.parseInt(width);
				iRowNumber = Integer.parseInt(saValue[0]);
				sValue = saValue[1];
				for (int i = (iWidth * 3); i > iRowNumber; i--) {
					tRowNumber.set(fileName + "~" + i);
					tValue.set(sValue.trim());
					output.collect(tRowNumber, tValue);
				}

				tRowNumber.set(fileName + "~" + iRowNumber);
				tValue.set("-1 " + sValue.substring(sValue.indexOf(" ")).trim());
				output.collect(tRowNumber, tValue);

			} catch (Exception e) {
				System.out.println("Error in Task 2 Map task" + e);
			}
		}
	}

	//Reducer class

	public static class AggregateReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
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
		Text tValue = new Text();

		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String sTemp;
			String sValue = "";
			int iNumofBits = 1;
			try {
				while (values.hasNext()) {
					sTemp = values.next().toString().trim();
					iNumofBits = iNumofBits + Integer.parseInt(sTemp.substring(0, sTemp.indexOf(" ")).trim());
					//	    		  System.out.println("num of bits-----"+Integer.parseInt(sTemp.substring(0,sTemp.indexOf(" ")).trim()));
					if (sTemp.substring(0, sTemp.indexOf(" ")).trim().equalsIgnoreCase("-1")) {
						sValue = sTemp.substring(sTemp.indexOf(" ")).trim();
					}
				}
				sValue = iNumofBits + " " + sValue;
				tValue.set(sValue);
				output.collect(key, tValue);
			} catch (Exception e) {
				System.out.println("Error in Task 2 Reduce task" + e);
			}
		}
	}

	//step 3 : Embeds the message value to the image pixels
	//mapper class

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

		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String[][] rowData = null;
			Double NewMSE, oldMSE;
			String[] rowSplit = null;
			String[] rowNum = key.toString().split("~");
			//			String fileName = ((FileSplit) reporter.getInputSplit()).getPath().getName();

			String sfileName = key.toString().substring(0, key.toString().indexOf("~") - 1);
			String height = sfileName.substring(sfileName.indexOf("_") + 1);
			String width = height.substring(0, height.indexOf("x"));
			height = height.substring(height.indexOf("x") + 1, height.indexOf("."));

			int iHeight = 0, iWidth = 0, iproduct = 0;
			try {
				iHeight = Integer.parseInt(height);
				iWidth = Integer.parseInt(width);
				iproduct = iHeight * iWidth;
			} catch (Exception e) {
				System.out.println("Error in getting Height and width in embed reducer " + e);
			}
			for (int i = 0; i < alMessage.size(); i++) {
				if (hmMSE.containsKey("Msg" + i + "#" + sfileName)) {
					//do nothing
				} else {
					hmMSE.put("Msg" + i + "#" + sfileName, 0.0);
				}
			}

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
					for (int i = 0; i < alMessage.size(); i++) {
						if (Integer.parseInt(rowNum[1]) % 2 == 0) {

							NewMSE = calculateBits(rowData, 1, imessageBit, i);
							NewMSE = NewMSE / iproduct;
							oldMSE = hmMSE.get("Msg" + i + "#" + sfileName);
							hmMSE.put("Msg" + i + "#" + sfileName, (oldMSE + NewMSE));
							//							System.out.println("MSE---------"+hmMSE.get("Msg"+i+"#"+sfileName)+"");
							output.collect(new Text("Msg" + i + "#" + key.toString()), new Text(
									writeArrayToString(rowData[1])));
						} else {
							NewMSE = calculateBits(rowData, 2, imessageBit, i);
							NewMSE = NewMSE / iproduct;
							oldMSE = hmMSE.get("Msg" + i + "#" + sfileName);
							hmMSE.put("Msg" + i + "#" + sfileName, (oldMSE + NewMSE));
							output.collect(new Text("Msg" + i + "#" + key.toString()), new Text(
									writeArrayToString(rowData[1])));
						}
					}

				}
			} catch (Exception e) {
				System.out.println("exception in reduce" + e);
			}
			//			System.out.println("key value-----"+key.toString().substring(key.toString().indexOf("~")+1));
			int rowNumber = Integer.parseInt(key.toString().substring(key.toString().indexOf("~") + 1));
			if (rowNumber == (iWidth * 3)) {
				for (int i = 0; i < alMessage.size(); i++) {
					output.collect(
							new Text("MSE~" + "Msg" + i + "#"
									+ key.toString().substring(0, key.toString().indexOf("~"))),
							new Text(hmMSE.get("Msg" + i + "#" + sfileName) + ""));
				}

			}

		}

		/**
		 * 
		 * @param inputString - array of row strings
		 * @param startBit - bit in which the embedding starts
		 * 
		 * @throws Exception
		 */
		public Double calculateBits(String[][] rowData, int startBit, int messagebit, int count) throws Exception {
			int d;
			int p1;
			int p2;
			Double MSE = 0.0;
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
						newBit = embed(oldBit, 1, messagebit, count);
						rowData[1][i] = newBit + "";
						messagebit = messagebit + 1;
						MSE = MSE + Math.pow((oldBit - newBit), 2);

					} else if (d % 2 == 1) {
						bits = (int) Math.floor(Math.log10(d) / 0.3010);
						oldBit = Integer.parseInt(rowData[1][i]);
						newBit = embed(oldBit, bits, messagebit, count);
						rowData[1][i] = newBit + "";
						messagebit = messagebit + bits;
						MSE = MSE + Math.pow((oldBit - newBit), 2);
					} else {
						bits = (int) (Math.floor(Math.log10(d) / 0.3010) - 1);
						oldBit = Integer.parseInt(rowData[1][i]);
						newBit = embed(oldBit, bits, messagebit, count);
						rowData[1][i] = newBit + "";
						messagebit = messagebit + bits;
						MSE = MSE + Math.pow((oldBit - newBit), 2);
					}
				}
				//				System.out.println("MSE in calculate bits ----" +MSE);
				return MSE;
			} catch (Exception e) {
				throw new Exception("Exception in calculateBits --- " + e);
			}

		}

		/**
		 * 
		 * @param iBit - int(value of current pixel)
		 * @param NumOfbits - int(number of bits that can be emedded)
		 * @param startbit - int (start position of message bits)
		 * @return
		 */
		public int embed(int iBit, int NumOfbits, int startbit, int count) {
			int iNewValue = 0;
			try {
				//				System.out.println("iBit in embed bit------- " +iBit);
				String[] imageBit = Integer.toBinaryString(iBit).toString().split("");
				imageBit = Arrays.copyOfRange(imageBit, 1, imageBit.length);

				int j = 0;
				String sNewvalue = "";
				int[] newMessage = new int[alMessage.get(count).length];
				newMessage = alMessage.get(count);
				//		    	System.out.println("newMessage----- " + newMessage[0]);
				if ((startbit + NumOfbits) > newMessage.length) {
					NumOfbits = newMessage.length - startbit;
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
						newImagebit[j] = newMessage[startbit] + "";
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
			} catch (Exception e) {
				e.printStackTrace();
			}
			//	    	System.out.println("iNewBit------ "+iNewValue);
			return iNewValue;

		}

		/**
		 * @param - JobConf
		 */

		@Override
		public void configure(JobConf job) {
			//iFile = job.get("map.input.file");
			//int temp = iFile.lastIndexOf("/");
			//iFile = iFile.substring(temp+1);
			//			int i = 0;
			Path[] SearchFiles = new Path[0];
			try {
				SearchFiles = DistributedCache.getLocalCacheFiles(job);
			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files: "
						+ StringUtils.stringifyException(ioe));
			}
			for (Path inFile : SearchFiles) {
				alMessage.add(addMessageToArray(inFile));

			}
		}

		/**
		 * 
		 * @param inFile - input file that contains the message
		 */

		public int[] addMessageToArray(Path inFile) {
			try (BufferedReader fis = new BufferedReader(new FileReader(inFile.toString()));) {
				String pattern = null;
				String sMesageInBits = "";
				while ((pattern = fis.readLine()) != null) {
					sMesageInBits = sMesageInBits + writeArrayToString(getMesageBits(pattern));
				}
				String[] saMessageBits = new String[sMesageInBits.trim().length()];
				saMessageBits = sMesageInBits.split(" ");
				int[] imessage = new int[saMessageBits.length];
				for (int i = 0; i < saMessageBits.length; i++) {
					imessage[i] = Integer.parseInt(saMessageBits[i]);
					System.out.print(imessage[i]);
				}
				return imessage;
			} catch (IOException ioe) {
				System.err.println("Caught exception in addMessageToArray IOE" + StringUtils.stringifyException(ioe));
				return null;
			} catch (IndexOutOfBoundsException in) {
				System.err.println("Caught exception in addMessageToArray IN" + StringUtils.stringifyException(in));
				return null;
			} catch (NumberFormatException num) {
				System.err.println("Caught exception in addMessageToArray NUM" + StringUtils.stringifyException(num));
				return null;
			}

		}

		/**
		 * 
		 * @param sMessage - String(message)
		 * convert string into bits of integer and stores in message[]
		 * @return
		 */

		public static String[] getMesageBits(String sMessage) {

			String sBit = "", bit;
			for (int i = 0; i < sMessage.length(); i++) {
				bit = Integer.toBinaryString(sMessage.charAt(i));
				if (bit.length() == 7) {
					sBit = sBit + bit;

				} else {
					sBit = sBit + "0" + bit;
				}
			}

			String[] bitArray = new String[sBit.length()];
			for (int i = 0; i < sBit.length(); i++) {

				bitArray[i] = Character.toString(sBit.charAt(i));
			}

			return bitArray;
		}
	}

	/**
	 * 
	 * @author Vignesh Vijayabasker
	 * performs manipulation and creates file as specified
	 *
	 */

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

	/**
	 * @param - String[] (intial args of the funciton)
	 * more like main function
	 */

	@Override
	public int run(String[] args) throws Exception {

		//String sPath = "/user/cloudera/project/intermediate";
		JobConf conf = new JobConf(NewImgSteg.class);
		conf.setJobName("Steg");

		JobConf conf2 = new JobConf(NewImgSteg.class);
		conf2.setJobName("Embed");

		List<String> other_args = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			if ("-message".equalsIgnoreCase(args[i])) {
				DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf2);
				conf.setBoolean("message.input.file", true);
			} else {
				other_args.add(args[i]);
			}
		}
		System.out.println("message true-----------" + conf.getBoolean("message.input.file", true));
		System.out.println("message false-----------" + conf.getBoolean("message.input.file", false));
		//		DistributedCache.addCacheFile(new Path(args[2]).toUri(), conf);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(BitCalcMap.class);
		conf.setReducerClass(BitCalcReduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(outputClass.class);

		FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(0) + "/inter"));
		//JobClient.runJob(conf);

		//job 2 - Aggregate
		JobConf conf1 = new JobConf(NewImgSteg.class);
		conf1.setJobName("Aggregate");

		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(Text.class);

		conf1.setMapperClass(AggregateMap.class);
		conf1.setReducerClass(AggregateReduce.class);

		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(outputClass.class);

		FileInputFormat.setInputPaths(conf1, new Path(other_args.get(0) + "/inter"));
		FileOutputFormat.setOutputPath(conf1, new Path(other_args.get(0) + "/inter1"));

		//JobClient.runJob(conf1);

		//job 3 - EMbed

		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(Text.class);

		conf2.setMapperClass(EmbedBitMap.class);
		conf2.setReducerClass(EmbedBitReduce.class);

		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(outputClass.class);

		FileInputFormat.setInputPaths(conf2, new Path(other_args.get(0) + "/inter1"));
		FileOutputFormat.setOutputPath(conf2, new Path(other_args.get(1)));
		JobClient.runJob(conf2);
		return 0;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new NewImgSteg(), args);
		System.exit(res);
	}

}
