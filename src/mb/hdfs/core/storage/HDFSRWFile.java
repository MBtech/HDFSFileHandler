/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.core.storage;

import java.io.IOException;
import java.util.logging.Level;
import mb.hdfs.aux.PathConstruction;
import mb.hdfs.core.filemanager.FileMngr;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PieceOps class allows to read and write pieces in HDFS It accumulates the
 * pieces until they are big enough (BlockSize of HDFS) and then writes it to
 * the HDFS.
 *
 * @author Muhammad Bilal <mubil@kth.se>
 */
public class HDFSRWFile implements Storage {

    private final String fileName, folderName;
    private final int blockSize;
    private final int pieceSize;
    private FSDataOutputStream fdos;
    private final FileSystem hdfs;
    private final Path P;
    private static final Logger logger = LoggerFactory.getLogger(HDFSRWFile.class);
    public HDFSRWFile(String folderName, String fileName, long fileBlockSize, 
            int blockSize, int pieceSize, FileMngr hashFileMngr)
            throws IOException {
        this.fileName = fileName;
        this.folderName = folderName;
        this.blockSize = blockSize;
        this.pieceSize = pieceSize;
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://172.31.16.234:9000");
        //conf.set("fs.default.name", "hdfs://172.31.16.234:9000"); Deprecated
        //conf.addResource("/home/mb/NetBeansProjects/HDFSFileHandler/core-site.xml");
        //conf.addResource("/home/mb/NetBeansProjects/HDFSFileHandler/hdfs-site.xml");
        hdfs = FileSystem.get(conf);
        P = PathConstruction.CreatePathAndFile(hdfs, folderName, fileName, true);
        fdos = hdfs.create(P, true, blockSize, (short) 2, fileBlockSize);
        fdos.close();
        if (blockSize % pieceSize != 0) {
            throw new IllegalArgumentException("Illegal pieceSize: Size should be multiple of blockSize");
        }
    }

    //TODO Change type of blockSize to long
    @Override
    public byte[] readPiece(int piecePos){
        FSDataInputStream fdis = null;
        try {
            Path filePath = PathConstruction.CreateReadPath(hdfs, folderName, fileName);
            fdis = new FSDataInputStream(hdfs.open(filePath));
            byte[] readPiece = new byte[pieceSize];
            fdis.readFully(pieceSize * piecePos, readPiece, 0, pieceSize);
            fdis.close();
            return readPiece;
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(HDFSRWFile.class.getName()).log(Level.SEVERE, null, ex);
        } 
        return null; //handle nu,,
    }

    @Override
    public void writePiece(int piecePos, byte[] piece){
        try {
            fdos = hdfs.append(P);
            fdos.write(piece);
            fdos.flush();
            fdos.close();
            //Without closing this write handle. Read in HDFS is giving problems
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(HDFSRWFile.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    @Override
    public void write(long offset, byte[] piece){
        try {
            fdos = hdfs.append(P);
            fdos.write(piece);
            fdos.flush();
            fdos.close();
            //Without closing this write handle. Read in HDFS is giving problems
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(HDFSRWFile.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    // have to include the usage in HDFS file managers 
    @Override
    public byte[] read(long offset, int length) {
        FSDataInputStream fdis = null;
        try {
            Path filePath = PathConstruction.CreateReadPath(hdfs, folderName, fileName);
            fdis = new FSDataInputStream(hdfs.open(filePath));
            byte[] readBuffer = new byte[length];
            FileStatus fs = hdfs.getFileStatus(filePath);
            logger.debug("Size of file" + fs.getLen());
            fdis.readFully(offset, readBuffer, 0, length);
            fdis.close();
            return readBuffer;
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(HDFSRWFile.class.getName()).log(Level.SEVERE, null, ex);
        } 
        return null; //handle null
    }

}
