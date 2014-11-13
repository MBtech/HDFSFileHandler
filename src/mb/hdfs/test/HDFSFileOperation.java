/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
import java.util.TreeSet;
import mb.hdfs.aux.UnitConversion;
import mb.hdfs.core.filemanager.HDFSFileManager;
import mb.hdfs.core.piecetracker.HDFSPieceTracker;
import mb.hdfs.core.piecetracker.PieceTracker;
import mb.hdfs.datagen.DataGen;;
import mb.hdfs.core.storage.HDFSStorageFactory;
import mb.hdfs.core.storage.Storage;
/**
 * 
 * @author Muhammad Bilal <mubil@kth.se>
 */
    public class HDFSFileOperation {
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
        
        
        //Testing Operations of HDFSStorage
        //NOTE: The write operations need to be close before read can be done.
        Storage s = HDFSStorageFactory.getExistingFile("MyTestFolder","MyTestFile",UnitConversion.mbToBytes(1),UnitConversion.kbToBytes(256));
        PieceTracker p = new HDFSPieceTracker(16);
        HDFSFileManager hpfo = new HDFSFileManager(s, p);
        
        for(int i = 0; i<12; i++){
            hpfo.writePiece(i, new DataGen().randDataGen(UnitConversion.kbToBytes(256)));
        }
        
        hpfo.readPiece(2);
        System.out.println("Reading done");
        System.out.println("Closing the file");  
        //Sending out of order packets
        Set n = new TreeSet(); 
        n.add(12);
        n.add(14);
        n.add(15);
        n.add(13);
        for(Object i:n){
            hpfo.writePiece((int)i, new DataGen().randDataGen(UnitConversion.kbToBytes(256)));

        }
        
        hpfo.readPiece(14);
        System.out.println("Reading done");
        System.out.println("Closing the file");   
        
        
    }

}
