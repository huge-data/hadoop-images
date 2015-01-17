package zx.soft.hdfs.images.stegnography;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.StringTokenizer;

public class Unsteg {

	public static String calculateBits(int[][] inputString, int startBit) throws Exception {
		int d;
		int p1;
		int p2;
		int p3, k, b;
		String values = "";
		String sMessage = "";
		int totalbits = 0;
		try (BufferedWriter bw = new BufferedWriter(new FileWriter("test.txt"));) {
			if (inputString.length != 2) {
				throw new Exception("Exception in calculate Bits length problem ");
			}
			for (int i = startBit; i < inputString[1].length; i = i + 2) {
				p1 = inputString[1][i - 1];
				p2 = inputString[0][i];
				d = Math.abs(p1 - p2);
				if (d < 3) {
					totalbits = 1;
				} else if (d % 2 == 1) {
					totalbits = (int) Math.floor(Math.log10(d) / 0.3010);
				} else {
					totalbits = (int) (Math.floor(Math.log10(d) / 0.3010) - 1);
				}
				p3 = inputString[1][i];
				//System.out.println();
				k = (int) Math.pow(2, totalbits);
				b = p3 % k;
				//				System.out.print("original b---- "+ b);
				//				System.out.println(i+"-- "+p1+"----"+p2);
				b = Math.abs(b);
				String[] sBits = Integer.toBinaryString(b).split("");
				if ((totalbits + 1) != sBits.length) {
					System.out.print(" in if ");
					sBits = addWhiteSpace(sBits, totalbits);
				}
				for (int j = 0; j < sBits.length; j++) {
					sMessage = sMessage + sBits[j];
				}
				//				System.out.println(" p3---- "+ p3+" p2---- "+ p2+" p1---- "+ p1+" totalbits----"+totalbits +" binary value----"+ sMessage + " binary bits----"+sBits.length );
				values = values + sMessage;
				bw.write(b + "");
				//System.out.println(b);
			}
			bw.close();
		} catch (Exception e) {
			throw new Exception("Exception in calculateBits --- " + e);
		}
		return values;
	}

	public static String[] addWhiteSpace(String[] imageBit, int totalbits) {
		String[] newImageBit = new String[totalbits];
		imageBit = Arrays.copyOfRange(imageBit, 1, imageBit.length);
		int bitDiffernce = totalbits - (imageBit.length);
		//   	System.out.println("difference==" + bitDiffernce);
		int j = 0;
		for (int i = 0; i < newImageBit.length; i++) {
			if (bitDiffernce > 0) {
				newImageBit[i] = 0 + "";
				bitDiffernce--;
			} else {
				//    			System.out.println("in else");
				newImageBit[i] = imageBit[j];
				j++;
			}
		}
		return newImageBit;
	}

	public static int[][] readFileToArray(String filename) throws Exception {
		String line;
		int rowCount = 0;
		int[][] aValue = new int[1536][512];
		StringTokenizer st = null;
		int height = 0, width = 0;
		FileReader fread = new FileReader(filename);

		int colValue = 0;
		try (BufferedReader bf = new BufferedReader(fread);) {
			while ((line = bf.readLine()) != null) {
				width = 0;
				rowCount = 0;
				st = new StringTokenizer(line);

				while (st.hasMoreTokens()) {
					if (rowCount == 0) {
						colValue = Integer.parseInt(st.nextToken());
					} else {
						aValue[colValue - 1][width] = Integer.parseInt(st.nextToken());
						width = width + 1;
					}
					rowCount++;

				}
				height = height + 1;
			}
		}

		int[][] finalvalue = new int[height][width];

		for (int i = 0; i < height; i++) {
			for (int j = 0; j < width; j++) {
				finalvalue[i][j] = aValue[i][j];
			}
		}

		return finalvalue;
	}

	public static String getMessage(int[] bitArray) {
		String sMessage = "";
		for (int i = 0; i < bitArray.length / 7; i++) {
			sMessage = sMessage
					+ (char) Integer.parseInt((bitArray[(7 * i)] + "" + bitArray[((7 * i) + 1)] + ""
							+ bitArray[((7 * i) + 2)] + "" + bitArray[((7 * i) + 3)] + "" + bitArray[((7 * i) + 4)]
							+ "" + bitArray[((7 * i) + 5)] + "" + bitArray[((7 * i) + 6)]), 2);

		}
		sMessage = sMessage.substring(0, sMessage.indexOf("/0"));
		return sMessage;

	}

	public static void main(String args[]) throws Exception {
		String filePath = "Msg0#out1_224x187.txt";
		String height = filePath.substring(filePath.indexOf("_") + 1);
		height = height.substring(0, height.indexOf("x"));
		int iHeight = Integer.parseInt(height);
		int[][] values = readFileToArray(filePath);
		int[][] test = new int[2][values[1].length];
		int k;
		int[] binaryValues;
		String unstegMessage = "";
		String binaryString = "";
		String toChar = "";
		int dummy = 0;
		for (int i = 0; i < (iHeight * 3); i++) {
			test[0] = values[i];
			test[1] = values[i + 1];
			dummy = dummy + calculateBits(test, (i % 2) + 1).length();
			binaryString = binaryString + calculateBits(test, (i % 2) + 1);
			//			System.out.println("binaryString =-- "+binaryString);
			k = binaryString.length() / 7;
			toChar = binaryString.substring(0, (k * 7));
			binaryValues = new int[toChar.length()];
			for (int j = 0; j < toChar.length(); j++) {
				binaryValues[j] = Integer.parseInt(toChar.charAt(j) + "");
			}
			unstegMessage = unstegMessage + getMessage(binaryValues);
			if (unstegMessage.indexOf("/0") != -1) {
				break;
			}
			binaryString = binaryString.substring((k * 7), binaryString.length());
		}
		//		System.out.println("dummy --- "+dummy);
		System.out.println("unstegMessage " + unstegMessage);
	}

}
