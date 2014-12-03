/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.core.storage;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import mb.hdfs.aux.PathConstruction;
import mb.hdfs.core.filemanager.FileManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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

    public HDFSRWFile(String folderName, String fileName, int blockSize, int pieceSize, FileManager hashFileMngr)
            throws IOException {
        this.fileName = fileName;
        this.folderName = folderName;
        this.blockSize = blockSize;
        this.pieceSize = pieceSize;
        Configuration conf = new Configuration();
        conf.addResource("/home/mb/NetBeansProjects/HDFSFileHandler/core-site.xml");
        conf.addResource("/home/mb/NetBeansProjects/HDFSFileHandler/hdfs-site.xml");
        hdfs = FileSystem.get(conf);
        P = PathConstruction.CreatePathAndFile(hdfs, folderName, fileName, true);
        fdos = hdfs.create(P, true, blockSize, (short) 1, blockSize);
        fdos.close();
        if (blockSize % pieceSize != 0) {
            throw new IllegalArgumentException("Illegal pieceSize: Size should be multiple of blockSize");
        }
    }

    //TODO Change type of blockSize to long
    @Override
    public byte[] readPiece(int piecePos) throws IOException, NoSuchAlgorithmException {
        Path filePath = PathConstruction.CreateReadPath(hdfs, folderName, fileName);
        FSDataInputStream fdis = new FSDataInputStream(hdfs.open(filePath));
        byte[] readPiece = new byte[pieceSize];
        fdis.readFully(pieceSize * piecePos, readPiece, 0, pieceSize);
        fdis.close();
        return readPiece;
    }

    @Override
    public byte[] readBlock(int blockPos) throws IOException {
        Path filePath = PathConstruction.CreateReadPath(hdfs, folderName, fileName);
        FSDataInputStream fdis = new FSDataInputStream(hdfs.open(filePath));
        byte[] readBlock = new byte[blockSize];
        System.out.println(fdis.available());
        FileStatus fs = hdfs.getFileStatus(filePath);
        System.out.println("Size of file " + fs.getLen());
        //System.out.println(blockSize);
        System.out.println("FileName " + fileName + " and FolderName " + folderName);
        System.out.println("Reading from " + (blockSize * blockPos) + " to " + (blockSize * blockPos + blockSize));
        fdis.readFully(blockSize * blockPos, readBlock, 0, blockSize);
        fdis.close();
        /**
        int current = 0;
        int left = blockSize;
        while (left > 0) {
            int n = fdis.read(blockSize * blockPos, readBlock, current, left);
            left = left - n;
            current = current + n;
            System.out.println(n);
            try { 
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(HDFSRWFile.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        * **/
        return readBlock;
    }

    //TODO There is no need for the piecePos in this case. As the HDFS only provides the append
    // operation
    // TODO change the blockSize type to long instead of int 

    @Override
    public void writePiece(int piecePos, byte[] piece) throws IOException, NoSuchAlgorithmException {
        fdos = hdfs.append(P);
        fdos.write(piece);
        fdos.flush();
        fdos.close();
        //Without closing this write handle. Read in HDFS is giving problems
    }

}
