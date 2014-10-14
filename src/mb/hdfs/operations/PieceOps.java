/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.operations;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import mb.hdfs.aux.PathConstruction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author mb
 */
public class PieceOps implements PieceOperations{
    private final String fileName, folderName;
    private final int blockSize;
    private final int pieceSize;
    //private int ppb;
    private int count;
    private byte [] writeArray;
    private final FSDataOutputStream fdos;
    private FileSystem hdfs;
    public PieceOps(String folderName, String fileName, int blockSize, int pieceSize) throws IOException{
        this.fileName = fileName;
        this.folderName = folderName;
        this.blockSize = blockSize;
        this.pieceSize = pieceSize;
        writeArray= new byte [blockSize];
        this.count = 0;
        
        hdfs = FileSystem.get(new Configuration());
        Path[] P = PathConstruction.CreatePathAndFile(hdfs, folderName, fileName, true);
        //Uncomment if you want to delete the created paths after test run
        //hdfs.deleteOnExit(P[0]);
        //hdfs.deleteOnExit(P[1]);
        
        fdos= hdfs.create(P[0],true,blockSize,(short)3,blockSize);
        if (blockSize%pieceSize != 0){
        //this.ppb = (int)blockSize/pieceSize;
        throw new IllegalArgumentException("Illegal pieceSize: Size should be multiple of blockSize");
        }
    }
    public void close() throws IOException{
        this.fdos.close();
        hdfs.close();
        
    }
    private byte [] readPiece(FileSystem hdfs, Path filePath, int piecePos, int blockSize) throws IOException{
        FSDataInputStream fdis = new FSDataInputStream(hdfs.open(filePath));
        byte [] readByte = new byte[pieceSize];
        //Data to be discarded
        for(int i=0; i<piecePos; i++){
            fdis.read(readByte, 0, pieceSize);
        }
        fdis.read(readByte, 0, pieceSize);
        System.out.println("Exiting read function");
        return readByte;
    }
    
    private void writePiece(FileSystem hdfs, Path filePath, int piecePos, int blockSize, byte[] piece) throws IOException{
        System.out.println("Size of writeArray " + writeArray.length);
        
        System.arraycopy(piece, 0, writeArray, count, pieceSize);
        count += pieceSize;
        System.out.println("The current index after increment " + count );
        
        if(count>=this.blockSize){
            System.out.println("Cleaning the array");
            fdos.write(writeArray); 
            count = 0;
            writeArray = new byte[blockSize];
            //fdos.close(); //Should the file be closed here or should there be a separate function to close it
        }
        
        
    }
    
    //TODO Change type of blockSize to long
    @Override
    public byte[] readPiece(int piecePos) throws IOException{
        FileSystem hdfs = FileSystem.get(new Configuration());
        Path [] P = new Path[2];
        P = PathConstruction.CreateReadPath(hdfs, folderName, fileName);
        Path filePath = P[0], hashFilePath = P[1];
        //blockSize = (int)hdfs.getFileStatus(filePath).getBlockSize();
        return readPiece(hdfs, filePath, piecePos, blockSize);
    }

    //TODO There is no need for the piecePos in this case. As the HDFS only provides the append
    // operation
    // TODO change the blockSize type to long instead of int 
    @Override
    public void writePiece(int piecePos, byte[] piece) throws IOException{
        FileSystem hdfs = FileSystem.get(new Configuration());
        Path [] P = new Path[2];
        P = PathConstruction.CreatePathAndFile(hdfs, folderName, fileName, false);
        Path filePath = P[0], hashFilePath = P[1];
        //blockSize = (int)hdfs.getFileStatus(filePath).getBlockSize();
        writePiece(hdfs, filePath, piecePos, blockSize, piece);
    }
    
}
