package zx.soft.hdfs.images.mosaic;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created by roinir on 18/06/2014.
 */
public class ImagePyramid {

    ArrayList<ImagePyramidBin> bins = new ArrayList<ImagePyramidBin>();

    public ImagePyramid(ArrayList<ImagePyramidBin> bins) {
        this.bins = bins;
    }

    public void toMapFile(String path) throws IOException {
        File file = new File(path);

        // if file doesnt exists, then create it
        if (!file.exists()) {
            file.createNewFile();
        }

        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(fw);
            HashSet<String> names = collectTileNames();
            for(String name : names){
                String line = name + ":" + collectBinTilesByName(name);
                bw.write(line);
                bw.newLine();
            }
        }finally {
            if(bw != null) bw.close();
        }
    }

    private HashSet<String> collectTileNames(){
        HashSet<String> names = new HashSet<String>();
        for (ImagePyramidBin bin : bins) {
            names.addAll(bin.getNames());
        }
        return names;
    }

    private String collectBinTilesByName(String name){
        String binsString = "";
        for (ImagePyramidBin bin : bins) {
            PyramidBinTile tile = bin.getTile(name);
            if (tile != null){
                binsString += tile.toString();
            }
        }
        return binsString;
    }
}
