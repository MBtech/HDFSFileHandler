/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.main;

import mb.hdfs.operations.BlockOps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import mb.hdfs.datagen.DataGen;
import mb.hdfs.operations.PieceOps;
import mb.hdfs.operations.StreamingOps;
/**
 *
 * @author mb
 */
public class HDFSFileOperation {
    public static int mbToBytes(int mbBlockSize){
        return mbBlockSize*1024*1024;
    }
    
    public static int kbToBytes(int kbBlockSize){
        return kbBlockSize*1024;
    }
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws NoSuchAlgorithmException,IOException {
        FileSystem hdfs = FileSystem.get(new Configuration());

            //Print the home directory
        System.out.println("Home folder -" + hdfs.getHomeDirectory());

            // Create & Delete Directories
        Path workingDir = hdfs.getWorkingDirectory();

        Path newFolderPath = new Path("/MyDataFolder");

        newFolderPath = Path.mergePaths(workingDir, newFolderPath);

        if (hdfs.exists(newFolderPath)) {

            //Delete existing Directory
            hdfs.delete(newFolderPath, true);

            System.out.println("Existing Folder Deleted.");

        }

        hdfs.mkdirs(newFolderPath);     //Create new Directory

        System.out.println("Folder Created.");

            //Copying File from local to HDFS
        Path localFilePath = new Path("/home/mb/localdata/datafile1.txt");

        Path hdfsFilePath = new Path(newFolderPath + "/dataFile1.txt");

        hdfs.copyFromLocalFile(localFilePath, hdfsFilePath);

        System.out.println("File copied from local to HDFS.");

            //Copying File from HDFS to local
        localFilePath = new Path("/home/mb/hdfsdata/datafile1.txt");

        hdfs.copyToLocalFile(hdfsFilePath, localFilePath);

        System.out.println("Files copied from HDFS to local.");
        
        /**
        StreamingOps hsfo = new StreamingOps();
        hsfo.hdfsWriteData("MyTestFolder","MyTestFile",HDFSFileOperation.mbToBytes(1));
        hsfo.hdfsReadData("MyTestFolder", "MyTestFile",HDFSFileOperation.mbToBytes(1));
        **/
        
        /**
        BlockOps hbfo = new BlockOps();
        hbfo.hdfsWriteData("MyTestFolder","MyTestFile",HDFSFileOperation.mbToBytes(1));
        hbfo.hdfsReadData("MyTestFolder", "MyTestFile",HDFSFileOperation.mbToBytes(1));
        **/
        
        
        PieceOps hpfo = new PieceOps("MyTestFolder","MyTestFile",HDFSFileOperation.mbToBytes(1),HDFSFileOperation.kbToBytes(256));
        for(int i = 0; i<12; i++){
            hpfo.writePiece(i, new DataGen().randDataGen(HDFSFileOperation.kbToBytes(256)));
        }
        hpfo.close();
        hpfo.readPiece(2);
        System.out.println("Reading done");
        System.out.println("Closing the file");
        //hpfo.close();
        
        //System.out.println("");
       
        
    }

}
