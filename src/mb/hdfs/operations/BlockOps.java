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
import java.util.Arrays;
import mb.hdfs.aux.Logger;
import mb.hdfs.datagen.DataGen;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import mb.hdfs.aux.PathConstruction;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 * BlockOps class provides full block read and write operations.
 * With a fully functional PieceOps there is no particular need for BlockOps
 * So it has been deprecated 
 * @author mb
 */
@Deprecated
public class BlockOps implements Operations{
    /**
     * It's BlockOps not BlackOps 
     * BlockOps provides block read, write and hashing for HDFS files.
     * BlockOps has been deprecated
     * @param folderName Name of the folder for the file to be written
     * @param fileName Name of the file to be written
     * @param blockSize Block size (in bytes)
     * @throws NoSuchAlgorithmException
     * @throws IOException 
     */
    final boolean LOG = true;
    
    
    @Override
    public void hdfsWriteData(String folderName, String fileName, int blockSize) throws NoSuchAlgorithmException, IOException {
        FileSystem hdfs = FileSystem.get(new Configuration());
        Path [] P = new Path[2];
        P = PathConstruction.CreatePathAndFile(hdfs, folderName, fileName, true);
        Path filePath = P[0], hashFilePath = P[1];
        //Writing data to a HDFS file
        FSDataOutputStream fsOutStream = hdfs.create(filePath, true, blockSize, (short) 3, blockSize);
        DataGen dg = new DataGen();
        byte [] randbyt = new byte[0], byt = new byte[0];
        byte [] tmp; 
        while(byt.length<blockSize){
            randbyt = dg.randDataGen(50000);
            //Logger.log(byt.length, LOG);
            //Logger.log(randbyt.length,LOG);
            //Logger.log(randbyt, LOG);
            tmp = byt;
            byt = new byte[tmp.length+randbyt.length];
            //Logger.log(byt.length,LOG);
            System.arraycopy(tmp, 0, byt, 0, tmp.length);
            System.arraycopy(randbyt, 0, byt, byt.length-randbyt.length, randbyt.length);
            //System.out.print(byt);
        }
        //int diff;
        //byte [] nxtbyt = new byte[byt.length-blockSize];
        /**if((diff = byt.length-blockSize)>0){
            System.arraycopy(byt, blockSize, nxtbyt, 0, diff);
        }**/
        
        //TODO extra bytes are being written to the HDFS (more than a block size)
        //Write to the file stream
        fsOutStream.write(byt);
        Logger.log("Length of the file written is " + byt.length, LOG);
        
        //Hashing 
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(byt);
          
        //Copying the remainder of bytes back to main array
        //byt = nxtbyt;
        
        FSDataOutputStream fsHOutputStream = hdfs.create(hashFilePath);
        
        //System.out.write(md.digest());
        byte [] mdbytes = md.digest();
        //String s = new String(hash, "UTF-8");
        
        //Converting the byte array from Message Digest to hex string 
        //If not done it leads to problems with reading hash bytes later on
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < mdbytes.length; i++) {
          sb.append(Integer.toString((mdbytes[i] & 0xff) + 0x100, 16).substring(1));
        }
 
        System.out.println("Hex format : " + sb.toString());
 
       //convert the byte to hex format method 2
        /**
        StringBuffer hexString = new StringBuffer();
    	for (int i=0;i<mdbytes.length;i++) {
    	  hexString.append(Integer.toHexString(0xFF & mdbytes[i]));
    	}
 
    	System.out.println("Hex format : " + hexString.toString());
        **/
        
        
        fsHOutputStream.writeBytes(sb.toString());
        
        fsOutStream.flush();
        fsHOutputStream.flush();
        fsOutStream.close();
        fsHOutputStream.close();

        System.out.println("Written data to HDFS file.");
    }
    
    @Override
    public void hdfsReadData(String folderName, String fileName, int blockSize) throws IOException, NoSuchAlgorithmException {
        FileSystem hdfs = FileSystem.get(new Configuration());
        Path [] P = new Path[2];
        P = PathConstruction.CreateReadPath(hdfs, folderName, fileName);
        Path filePath = P[0], hashFilePath = P[1];
        //Reading data From HDFS File
        System.out.println("Reading from HDFS file.");

        //TODO Change the BufferedReader to FSDataInputStream
        BufferedReader bfr = new BufferedReader(
                new InputStreamReader(hdfs.open(filePath)));

        String str;
        byte [] data = new byte[2*blockSize];
        int cpos = 0;
        while ((str = bfr.readLine()) != null) {
            //byte [] tmp = str.getBytes(); 
            System.out.println(str.length());
            //System.out.write(str.getBytes());
            System.arraycopy(str.getBytes(), 0, data, cpos, str.length());
            cpos = cpos +str.length();
            //System.out.println(cpos);
        }
        Logger.log("Length of the file read using readLine is "+ cpos, LOG);
       
        bfr.close();
       
        FSDataInputStream fhis = hdfs.open(hashFilePath);
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        
        byte [] hashBytes = new byte[64];
        int iread=0;       
        while(iread<64){
            
            iread += fhis.read(hashBytes,iread,64-iread);
            System.out.println("Number of bytes read are "+ iread);
        }
        
        //TODO: Make it more generalized 
        //Right now the code only handles one hash in the file
        
        System.out.write(hashBytes);        
        //System.out.println(cpos);
        md.update(data,0,cpos);
           
        byte [] mdbytes = md.digest();
        
        //Convert the hash from byte array to hex string for comparison
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < mdbytes.length; i++) {
          sb.append(Integer.toString((mdbytes[i] & 0xff) + 0x100, 16).substring(1));
        }
        
        System.out.println("Hex format : " + sb.toString());
               
        if(Arrays.equals(hashBytes,sb.toString().getBytes())){
            System.out.println("Match Successful");
        }
       
    }
}
