/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.operations;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import mb.hdfs.aux.Logger;
import mb.hdfs.aux.PathConstruction;
import mb.hdfs.datagen.DataGen;
import mb.hdfs.operations.Operations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author mb
 */
public class StreamingOps implements Operations{
    
    final boolean LOG = false;

    
    @Override
    public void hdfsWriteData(String folderName, String fileName, int blockSize) throws NoSuchAlgorithmException,IOException {
        FileSystem hdfs = FileSystem.get(new Configuration());
        Path [] P = new Path[2];
        P = PathConstruction.CreatePathAndFile(hdfs, folderName, fileName);
        Path newFilePath = P[0], newHashFilePath = P[1];
        //Writing data to a HDFS file
        FSDataOutputStream fsOutStream = hdfs.create(newFilePath, true, blockSize, (short) 3, blockSize);
        DataGen dg = new DataGen();
        byte [] randbyt = new byte[0],byt = new byte[0];
        while(byt.length<blockSize){
            randbyt = dg.randDataGen();
            Logger.log(byt.length,LOG);
            Logger.log(randbyt.length,LOG);
            byt = new byte[byt.length+randbyt.length];
            Logger.log(byt.length,LOG);
            System.arraycopy(randbyt, 0, byt, byt.length-randbyt.length, randbyt.length);
            //Write to the file stream
            fsOutStream.write(randbyt);
            //System.out.print(byt);
        }
        int diff;
        byte [] nxtbyt = new byte[byt.length-blockSize];
        if((diff = byt.length-blockSize)>0){
            System.arraycopy(byt, blockSize, nxtbyt, 0, diff);
        }
        
        //Hashing 
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(byt);
        
        //log(md.digest());
        
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
