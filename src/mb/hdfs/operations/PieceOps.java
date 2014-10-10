/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.operations;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import mb.hdfs.aux.PathConstruction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;

/**
 *
 * @author mb
 */
public class PieceOps implements PieceOperations{
    private String fileName, folderName;
    private int blockSize;
    public PieceOps(String folderName, String fileName, int blockSize){
        this.fileName = fileName;
        this.folderName = folderName;
        this.blockSize = blockSize;
    }
    
    public byte [] readPiece(FileSystem hdfs, Path filePath, int piecePos, int blockSize) throws IOException{
        BufferedReader bfr = new BufferedReader(
                new InputStreamReader(hdfs.open(filePath)));
        FSDataInputStream fdis = new FSDataInputStream(hdfs.open(filePath));
        byte [] readByte = new byte[blockSize];
        
       
        char [] rchar = new char[blockSize];
        for(int i=0; i<piecePos; i++){
            fdis.read(readByte, 0, blockSize);
        }
        fdis.read(readByte, 0, blockSize);
        
        //fdis.readFully(readByte, blockSize, piecePos);
        //if(bfr.read(rchar, piecePos*blockSize, blockSize)>0){    
        //}
        
        return readByte;
    }
    
    //TODO Change type of blockSize to long
    @Override
    public byte[] readPiece(int piecePos) throws IOException{
        FileSystem hdfs = FileSystem.get(new Configuration());
        Path [] P = new Path[2];
        P = PathConstruction.CreateReadPath(hdfs, folderName, fileName);
        Path filePath = P[0], hashFilePath = P[1];
        blockSize = (int)hdfs.getFileStatus(filePath).getBlockSize();
        return readPiece(hdfs, filePath, piecePos, blockSize);
    }

    @Override
    public void writePiece(int piecePos, byte[] piece) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
