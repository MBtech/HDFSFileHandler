/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.operations;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import mb.hdfs.aux.PathConstruction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * PieceOps class allows to read and write pieces in HDFS
 * It accumulates the pieces until they are big enough (BlockSize of HDFS)
 * and then writes it to the HDFS.
 * @author mb
 */
public class PieceOps implements PieceOperations{
    private final String fileName, folderName;
    private final int blockSize;
    private final int pieceSize;
    private final int ppb;
    private int count;
    private byte [] writeArray;
    private FSDataOutputStream fdos, fhos;
    
    private final FileSystem hdfs;
    public PieceOps(String folderName, String fileName, int blockSize, int pieceSize) 
    throws IOException{
        this.fileName = fileName;
        this.folderName = folderName;
        this.blockSize = blockSize;
        this.pieceSize = pieceSize;
        writeArray= new byte [blockSize];
        this.count = 0;
        this.ppb = (int)blockSize/pieceSize;
        
        hdfs = FileSystem.get(new Configuration());
        Path[] P = PathConstruction.CreatePathAndFile(hdfs, folderName, fileName, true);
        //Uncomment if you want to delete the created paths after test run
        //hdfs.deleteOnExit(P[0]);
        //hdfs.deleteOnExit(P[1]);
        
        fdos= hdfs.create(P[0],true,blockSize,(short)3,blockSize);
        fhos = hdfs.create(P[1], true, blockSize, (short)3, blockSize);
        
        if (blockSize%pieceSize != 0){
            
            throw new IllegalArgumentException
                    ("Illegal pieceSize: Size should be multiple of blockSize");
        }
        
        //fdos.close();
        //fhos.close();
        //hdfs.close();
    }
    /**
     * Close the instance of Piece Operation Class
     * @throws IOException 
     */
    public void close() throws IOException{
        this.fdos.close();
        this.fhos.close();
        //hdfs.close();
        
    }
    
    public void open() throws IOException{
        Path[] P = PathConstruction.CreatePathAndFile(hdfs, folderName, fileName, true);
        fdos= hdfs.append(P[0]);
        fhos = hdfs.append(P[1]);
    }
    
    private byte [] hash(byte [] dataArray) throws NoSuchAlgorithmException{
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(dataArray);
        return md.digest();
    }
    
    /**
     * 
     * @param hdfs
     * @param filePath
     * @param piecePos
     * @param blockSize
     * @return
     * @throws IOException 
     */
    private byte [] readPiece
        (FileSystem hdfs, Path filePath, Path hashFilePath, int piecePos, int blockSize) 
                throws IOException, NoSuchAlgorithmException{
        // TODO: Should these be moved because hdfs open is being called with every read operation.
        FSDataInputStream fdis = new FSDataInputStream(hdfs.open(filePath));
        
        byte [] readByte = new byte[this.pieceSize];
        byte [] readBlock = new byte[blockSize];
        byte [] hashByte = new byte[64];
        byte [] readHashByte;
        //Blocks to be discarded
        int blockPos = (int)Math.ceil(piecePos/ppb); // Review it
        System.out.println("No. of pieces Per block is "+ ppb);
        System.out.println("Block position of concern is "+ blockPos);
        int discardBlocks = blockPos - 1; //Review it
        int ppInBlock = piecePos % ppb; //Review it 
        System.out.println("Piece position in block is "+ ppInBlock);
        System.out.println(fdis.available());
        for(int i=0; i<=discardBlocks; i++){
            fdis.read(readBlock, 0, blockSize);
            System.out.println("No. of blocks discarded "+ i+1);
        }
        
        //fdis.mark(blockSize); // mark the start of the block from where we want to read
        int iread=0;
        while(iread<blockSize){
            iread += fdis.read(readBlock, iread, blockSize-iread);
            //System.out.println(iread);
        }
        System.out.println("Number of Read Bytes are: " + iread);
        
        System.out.println("Piece Size is " + pieceSize + " and start index is "+ ppInBlock*pieceSize);
        System.arraycopy(readBlock, ppInBlock*pieceSize, readByte, 0, pieceSize);
        
        /**
         *  for(int i=0; i<=discardBlocks;i++){
            fhis.read(hashByte, 0, 64);
            System.out.println("No. of hash blocks discarded "+ i+1);
        }
        **/
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(readBlock);
        readHashByte = md.digest();
            
        FSDataInputStream fhis = new FSDataInputStream(hdfs.open(hashFilePath));
        
        //fhis.read
        
        iread=0;       
        while(iread<64){
            
            iread += fhis.read(hashByte,iread,64-iread);
            System.out.println("Number of bytes read are "+ iread);
        }
        //fhis.read(hashByte, 0, 64); //Have to calculate hash and compare it with block hash
        //fhis.close();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < readHashByte.length; i++) {
          sb.append(Integer.toString((readHashByte[i] & 0xff) + 0x100, 16).substring(1));
        }
        System.out.println("Hex format of the hash of the data read: " + sb.toString());
        
        //System.out.write(hashByte);
        
        if(Arrays.equals(sb.toString().getBytes(), hashByte)){
            System.out.println("Match Successful");
            System.out.println("Exiting read function");
            return readByte;
        }
        else{
            return readByte;
            //throw new InvalidDataException("The hash results do no match");
        }
    }
    
    private void writePiece
        (FileSystem hdfs, Path filePath, Path hashFilePath, int piecePos, int blockSize, byte[] piece) 
                throws IOException, NoSuchAlgorithmException{
        //FSDataOutputStream fdos, fhos;
        //fdos = hdfs.append(filePath);
        //fhos = hdfs.append(hashFilePath);
        System.out.println("Size of writeArray " + writeArray.length);
        
        System.arraycopy(piece, 0, writeArray, count, pieceSize);
        count += pieceSize;
        System.out.println("The current index after increment " + count );
        byte [] hash;
        if(count>=this.blockSize){
            System.out.println("Cleaning the array");
            fdos.write(writeArray); 
            fdos.flush();
            hash = hash(writeArray);
            
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < hash.length; i++) {
                sb.append(Integer.toString((hash[i] & 0xff) + 0x100, 16).substring(1));
            }
            System.out.println("Hex format : " + sb.toString());
            fhos.writeBytes(sb.toString());
            
            fhos.flush();
           
            count = 0;
            writeArray = new byte[blockSize];
            //fdos.close(); //Should the file be closed here or should there be a separate function to close it
        }
        //fdos.close();
        //fhos.close();
        
    }
    
    //TODO Change type of blockSize to long
    @Override
    public byte[] readPiece(int piecePos) throws IOException, NoSuchAlgorithmException{
        FileSystem hdfs = FileSystem.get(new Configuration());
        Path [] P;
        P = PathConstruction.CreateReadPath(hdfs, folderName, fileName);
        Path filePath = P[0], hashFilePath = P[1];
        //blockSize = (int)hdfs.getFileStatus(filePath).getBlockSize();
        return readPiece(hdfs, filePath, hashFilePath,piecePos, blockSize);
    }

    //TODO There is no need for the piecePos in this case. As the HDFS only provides the append
    // operation
    // TODO change the blockSize type to long instead of int 
    @Override
    public void writePiece(int piecePos, byte[] piece) throws IOException, NoSuchAlgorithmException{
        FileSystem hdfs = FileSystem.get(new Configuration());
        Path [] P;
        P = PathConstruction.CreatePathAndFile(hdfs, folderName, fileName, false);
        Path filePath = P[0], hashFilePath = P[1];
        //blockSize = (int)hdfs.getFileStatus(filePath).getBlockSize();
        writePiece(hdfs, filePath, hashFilePath, piecePos, blockSize, piece);
    }
    
}
