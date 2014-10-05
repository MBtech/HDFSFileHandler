/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.filehandling;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import mb.hdfs.datagen.DataGen;
import mb.hdfs.interfaces.Operations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author mb
 */
public class BlockOps implements Operations{
    /**
     * 
     * @param folderName Name of the folder for the file to be written
     * @param fileName Name of the file to be written
     * @param blockSize Block size (in bytes)
     * @throws NoSuchAlgorithmException
     * @throws IOException 
     */
    public void hdfsWriteData(String folderName, String fileName, int blockSize) throws NoSuchAlgorithmException, IOException {
        FileSystem hdfs = FileSystem.get(new Configuration());
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
        //Writing data to a HDFS file
        FSDataOutputStream fsOutStream = hdfs.create(newFilePath, true, blockSize, (short) 3, blockSize);
        DataGen dg = new DataGen();
        byte [] randbyt =null, tmp =null;
       
        tmp = dg.randDataGen();
        System.out.println(tmp);
        byte [] byt = new byte[tmp.length];
        System.arraycopy(tmp, 0, byt, 0, tmp.length);
        while(byt.length<blockSize){
            randbyt = dg.randDataGen();
            System.out.println(byt.length);
            System.out.println(randbyt.length);
            byt = new byte[byt.length+randbyt.length];
            System.out.println(byt.length);
            System.arraycopy(randbyt, 0, byt, byt.length-randbyt.length, randbyt.length);
            //System.out.print(byt);
        }
        int diff = 0;
        byte [] nxtbyt = new byte[byt.length-blockSize];
        if((diff = byt.length-blockSize)>0){
            System.arraycopy(byt, blockSize, nxtbyt, 0, diff);
        }
        //Write to the file stream
        fsOutStream.write(byt);
        //Hashing
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(byt);
        System.out.println(md.digest());
        //Copying the remainder of bytes back to main array
        byt = nxtbyt;
        
        FSDataOutputStream fsHOutputStream = hdfs.create(newHashFilePath);
        
        fsHOutputStream.write(md.digest());

        fsOutStream.close();
        fsHOutputStream.close();

        System.out.println("Written data to HDFS file.");
    }
    @Override
    public void hdfsReadData(String folderName, String fileName) throws IOException {
        FileSystem hdfs = FileSystem.get(new Configuration());
        Path homePath = hdfs.getHomeDirectory();
        Path newFolderPath = new Path("/" + folderName);
        newFolderPath = Path.mergePaths(homePath, newFolderPath);
        Path newFilePath = new Path(newFolderPath + "/" + fileName);

        //Reading data From HDFS File
        System.out.println("Reading from HDFS file.");

        BufferedReader bfr = new BufferedReader(
                new InputStreamReader(hdfs.open(newFilePath)));

        String str = null;

        while ((str = bfr.readLine()) != null) {

            System.out.println(str);

        }
    }
}
