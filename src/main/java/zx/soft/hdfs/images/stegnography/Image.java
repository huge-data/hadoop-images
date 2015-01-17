package zx.soft.hdfs.images.stegnography;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.StringTokenizer;

import javax.imageio.ImageIO;

public class Image {

	/**
	 * @param filePath for the given filePath it calls getEachfile and process each file
	 * convert a image file in to a txt file
	 * @throws Exception
	 */
	public void getData(String filePath, String outFolderPath) throws Exception {
		ArrayList<String> alFileList = getEachFile(filePath);
		int sequence = 0;
		for (String sfileName : alFileList) {

			File file = new File(sfileName);
			BufferedImage image = ImageIO.read(file);
			FileWriter fstream = new FileWriter(outFolderPath + "out" + sequence + "_" + image.getHeight() + "x"
					+ image.getWidth() + ".txt");
			BufferedWriter out = new BufferedWriter(fstream);
			int value, red, blue, green;
			int imgHeight = image.getHeight();
			int[][] aValue = new int[image.getHeight() * 3][image.getWidth()];
			System.out.println(image.getHeight());
			System.out.println(image.getWidth());

			for (int i = 0; i < image.getHeight(); i++) {

				for (int j = 0; j < image.getWidth(); j++) {
					//System.out.println("i - "+i+"     j-- "+j);
					value = image.getRGB(j, i);
					red = (value >> 16) & 0xff;
					green = (value >> 8) & 0xff;
					blue = value & 0xff;
					aValue[i][j] = red;
					aValue[imgHeight + i][j] = green;
					aValue[(2 * imgHeight) + i][j] = blue;
				}
			}

			for (int i = 0; i < aValue.length; i++) {
				out.write((i + 1) + "\t");
				for (int j = 0; j < aValue[i].length; j++) {
					out.write("" + aValue[i][j] + " ");
				}
				out.write("\n");
			}

			out.close();
			sequence = sequence + 1;
		}
	}

	/**
	 * it can process only one file
	 * @param filename - value of the input text file
	 * @param imageFile - value of the output image file
	 * @throws Exception
	 */
	public void setData(String filename, String imageFile) throws Exception {
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
						System.out.println("colvalue --- " + colValue);
					} else {
						aValue[colValue][width] = Integer.parseInt(st.nextToken());
						width = width + 1;
					}
					rowCount++;

				}
				height = height + 1;
			}
		}
		BufferedImage newImage = new BufferedImage(width, height / 3, BufferedImage.TYPE_INT_RGB);
		//height =512;
		width--;
		System.out.println("width --- " + width);
		System.out.println("height --- " + height);

		for (int i = 0; i < height / 3; i++) {
			for (int j = 0; j < width; j++) {
				newImage.setRGB(j, i,
						((aValue[i][j] << 16) + (aValue[(height / 3) + i][j] << 8) + aValue[(2 * (height / 3)) + i][j]));
			}
		}

		File lena1 = new File(imageFile);
		ImageIO.write(newImage, "png", lena1);

	}

	/**
	 * 
	 * @param filePath (which is the folder path containing all the images
	 * @returns each file name as a path in the form of arraylist of strings
	 */
	public ArrayList<String> getEachFile(String filePath) {
		ArrayList<String> alFileList = new ArrayList<String>();
		File folder = new File(filePath);
		File[] fileList = folder.listFiles();
		for (File file : fileList) {
			alFileList.add(filePath + "/" + file.getName());
		}
		return alFileList;

	}

	public int[] getMesageBits(String sMessage) {

		String sBit = "", bit;
		for (int i = 0; i < sMessage.length(); i++) {
			bit = Integer.toBinaryString(sMessage.charAt(i));
			if (bit.length() == 7) {
				sBit = sBit + bit;

			} else {
				sBit = sBit + "0" + bit;
			}
		}

		int[] bitArray = new int[sBit.length()];
		for (int i = 0; i < sBit.length(); i++) {

			bitArray[i] = Integer.parseInt(Character.toString(sBit.charAt(i)));
		}

		return bitArray;
	}

	public String getMessage(int[] bitArray) {
		String sMessage = "";
		for (int i = 0; i < bitArray.length / 7; i++) {
			sMessage = sMessage
					+ (char) Integer.parseInt((bitArray[(7 * i)] + "" + bitArray[((7 * i) + 1)] + ""
							+ bitArray[((7 * i) + 2)] + "" + bitArray[((7 * i) + 3)] + "" + bitArray[((7 * i) + 4)]
							+ "" + bitArray[((7 * i) + 5)] + "" + bitArray[((7 * i) + 6)]), 2);

		}
		return sMessage;

	}

	public static void main(String args[]) throws Exception {
		Image i = new Image();
		if ("pre".equals(args[0])) {
			File f = new File("intImgs");
			f.mkdirs();
			i.getData(args[1], "intImgs/");
		} else {
			i.setData(args[1], args[2]);
		}

		i.setData("out0_184x153.txt", "ten.png");
		//    	File f = new File("intImgs");
		//    	f.mkdirs();
		//    	i.getData("images","intImgs/");
	}

}
