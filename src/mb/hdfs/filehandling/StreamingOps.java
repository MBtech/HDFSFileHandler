/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.filehandling;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;
import mb.hdfs.datagen.DataGen;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author mb
 */
public class StreamingOps {
    public void hdfsWriteData(String folderName, String fileName) throws NoSuchAlgorithmException,IOException {
        FileSystem hdfs = FileSystem.get(new Configuration());
        Path HomePath = hdfs.getHomeDirectory();
        Path newFolderPath = new Path("/" + folderName);
        newFolderPath = Path.mergePaths(HomePath, newFolderPath);

            //Creating a file in HDFS
        Path newFilePath = new Path(newFolderPath + "/" + fileName);

        hdfs.createNewFile(newFilePath);
        //Writing data to a HDFS file
        FSDataOutputStream fsOutStream = hdfs.create(newFilePath, true, 10000, (short) 3, 16777216);
        DataGen dg = new DataGen();
        for (int i = 0; i < 10; i++) {

            byte[] byt = dg.randDataGen();
            //Hashing 
            /*MessageDigest md = MessageDigest.getInstance("SHA1");
            md.update(byt);
            System.out.println(md.digest());
            */
            //Write to the file stream
            fsOutStream.write(byt);
        }
        fsOutStream.close();

        System.out.println("Written data to HDFS file.");
    }

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
