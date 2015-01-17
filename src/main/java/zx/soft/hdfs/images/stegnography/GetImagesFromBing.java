package zx.soft.hdfs.images.stegnography;

import java.awt.image.BufferedImage;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;

import javax.imageio.ImageIO;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class GetImagesFromBing {

	/**
	 * function - downloads image with the specified strURLs and stores in outpath
	 * @param strURL - string
	 * @param outpath - string
	 * @throws Exception
	 */
	public void downloadImageToLocalFolder(String strURL, String outpath) throws Exception {
		URL url = new URL(strURL);
		InputStream in = url.openStream();
		OutputStream out = new BufferedOutputStream(new FileOutputStream(outpath));
		for (int b; (b = in.read()) != -1;) {
			out.write(b);
		}
		out.close();
		in.close();
	}

	/**
	 * function - returns document using jsoup jar which is the parsed DOM of the input URL String
	 * @param urlString
	 * @return Document
	 * @throws Exception
	 */
	public Document getContent(String urlString) throws Exception {
		try {
			Document url = Jsoup.connect(urlString).get();
			return url;

		} catch (Exception e) {
			System.out.println("Error in getting the link output");
			return null;
		}

	}

	/**
	 * function - gets links of image form document and stores it in the string array
	 * @param document - Document
	 * @return - String[]
	 */
	public String[] getImageLink(Document document) {
		String[] imageLink;
		String eImageLink = document.select(".sg_pg .sg_u img[src~=(?i)\\.(png)]").toString();
		//		String eImageLink = document.select(".sg_pg .sg_u").toString();
		//		System.out.println(eImageLink);
		imageLink = eImageLink.split("<img");
		for (int i = 1; i < imageLink.length; i = i + 2) {
			//			System.out.println(imageLink[i].indexOf("src"));
			imageLink[i] = imageLink[i].substring(imageLink[i].indexOf("src=") + 5, imageLink[i].indexOf("style=") - 2);
			//			System.out.println(imageLink[i]);
		}
		return imageLink;
	}

	/**
	 * function - stores images in the links present in given string array to local folder
	 * @param URLs - String array
	 * @param outputPath - String
	 * @return - boolean
	 * @throws Exception
	 */
	public boolean storeImage(String[] URLs, String outputPath) throws Exception {
		try {
			int j = 1;
			for (int i = 1; i < URLs.length; i = i + 2) {
				downloadImageToLocalFolder(URLs[i], outputPath + "/img" + j + ".png");
				j++;
			}
			return true;
		} catch (Exception e) {
			System.out.println("Error in downloading images ");
			e.printStackTrace();
			return false;
		}

	}

	/**
	 * function - get all the files in the local folder and stores it in array list
	 * @param filePath (which is the folder path containing all the images
	 * @returns each file name as a path in the form of array list of strings
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

	/**
	 * changes the name of all the file in the array list
	 * @param filePath for the given filePath it calls getEachfile and process each file
	 * convert a image file in to a txt file
	 * @throws Exception
	 */
	public void changeFileName(String filePath) throws Exception {
		ArrayList<String> alFileList = getEachFile(filePath);
		for (String sfileName : alFileList) {
			//   		System.out.println(sfileName);
			File Oldfile = new File(sfileName);
			BufferedImage image = ImageIO.read(Oldfile);
			int imgHeight = image.getHeight();
			int imgWidth = image.getWidth();
			//           System.out.println(Oldfile.getName() + "----------" + imgHeight+ "-------------" + imgWidth);
			Oldfile.renameTo(new File(sfileName.substring(0, sfileName.indexOf(".")) + "_" + imgWidth + "x" + imgHeight
					+ ".png"));

		}
	}

	public static void main(String[] args) throws Exception {
		GetImagesFromBing gb = new GetImagesFromBing();
		//		System.out.println("http://www.bing.com/images/search?q="+args[0]+"&go=&qs=n&form=QBIR");
		//		BufferedWriter bf = new BufferedWriter(new FileWriter("text.txt"));
		//		bf.write(gb.getContent("http://www.bing.com/images/search?q=sachin+png&go=&qs=n&form=QBIR&pq=sachin+png&sc=8-10&sp=-1&sk=").toString());
		String[] URLlinksStrings = gb.getImageLink(gb.getContent("http://www.bing.com/images/search?q=" + args[0]
				+ "&go=&qs=n&form=QBIR"));
		//		for(int i=0;i<URLlinksStrings.length;i++)
		//		{
		//			System.out.println(URLlinksStrings[i]);
		//		}
		gb.storeImage(URLlinksStrings, "images");
		//		System.out.println(URLlinksStrings[1]);
		//		gb.downloadImageToLocalFolder("http://ts3.mm.bing.net/images/thumbnail.aspx?q=4559185639638182&amp;id=ec23a8390f93de06f70611c5d0a2b2b7&amp;url=http%3a%2f%2fi300.photobucket.com%2falbums%2fnn2%2falexandra0008%2fshakira.png", "images/img.png");
		gb.changeFileName("images");

	}

}
