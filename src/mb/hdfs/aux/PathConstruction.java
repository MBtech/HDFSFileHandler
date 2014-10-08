/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.aux;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author mb
 */
public class PathConstruction {
    /**
     * Create a File in HDFS and return the Path of the File as well as the 
     * corresponding Hash file
     * @param hdfs
     * @param folderName
     * @param fileName
     * @return
     * @throws IOException 
     */
    public static Path[] CreatePathAndFile(FileSystem hdfs, String folderName, String fileName) throws IOException{
        Path HomePath = hdfs.getHomeDirectory();
        Path newFolderPath = new Path("/" + folderName);
        newFolderPath = Path.mergePaths(HomePath, newFolderPath);
        
        //Delete the folder and the file if it already exists
        if (hdfs.exists(newFolderPath)){
           hdfs.delete(newFolderPath,true);
        }
        //Creating a file in HDFS
        Path newFilePath = new Path(newFolderPath + "/" + fileName);
        Path newHashFilePath = new Path(newFolderPath + "/" + fileName + "-hash");
        hdfs.createNewFile(newFilePath);
        hdfs.createNewFile(newHashFilePath);
        Path [] returnPath = new Path[]{newFilePath, newHashFilePath};
        return returnPath;
    }
    /**
     * Create and return a file path for reading 
     * @param hdfs FileSystem object
     * @param folderName Name of the folder in which the file is
     * @param fileName Name of the file to be read
     * @return 
     */
    public static Path[] CreateReadPath(FileSystem hdfs, String folderName, String fileName){
        Path homePath = hdfs.getHomeDirectory();
        Path newFolderPath = new Path("/" + folderName);
        newFolderPath = Path.mergePaths(homePath, newFolderPath);
        Path newFilePath = new Path(newFolderPath + "/" + fileName);
        Path newHashFilePath = new Path(newFolderPath + "/" + fileName + "-hash");
        Path [] returnPath = new Path[]{newFilePath, newHashFilePath};
        return returnPath;
    }
}
