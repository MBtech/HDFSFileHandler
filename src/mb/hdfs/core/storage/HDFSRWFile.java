/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.core.storage;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import mb.hdfs.aux.PathConstruction;
import mb.hdfs.core.filemanager.HDFSFileManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
    private static final int HASHSIZE = 64;
    private final String fileName, folderName;
    private final int blockSize;
    private final int pieceSize;
    private final FSDataOutputStream fdos;
    private final FileSystem hdfs;
    private final Path P;

    public HDFSRWFile(String folderName, String fileName, int blockSize, int pieceSize, HDFSFileManager hashFileMngr)
            throws IOException {
        this.fileName = fileName;
        this.folderName = folderName;
        this.blockSize = blockSize;
        this.pieceSize = pieceSize;
        hdfs = FileSystem.get(new Configuration());
        P = PathConstruction.CreatePathAndFile(hdfs, folderName, fileName, true);
        fdos = hdfs.create(P, true, blockSize, (short) 1, blockSize);
        if (blockSize % pieceSize != 0) {
            throw new IllegalArgumentException("Illegal pieceSize: Size should be multiple of blockSize");
        }
    }   

    //TODO Change type of blockSize to long
    @Override
    public byte[] readPiece(int piecePos) throws IOException, NoSuchAlgorithmException {
        Path filePath = PathConstruction.CreateReadPath(hdfs, folderName, fileName);
        FSDataInputStream fdis = new FSDataInputStream(hdfs.open(filePath));
        byte[] readPiece = new byte[this.pieceSize];
        fdis.readFully(pieceSize * piecePos, readPiece, 0, pieceSize);
        return readPiece;
    }

    @Override
    public byte [] readBlock(int blockPos) throws IOException{
        Path filePath = PathConstruction.CreateReadPath(hdfs, folderName, fileName);
        FSDataInputStream fdis = new FSDataInputStream(hdfs.open(filePath));
        byte[] readBlock = new byte[blockSize];
        fdis.readFully(blockSize * blockPos, readBlock, 0, blockSize);
        return readBlock;
    }
    //TODO There is no need for the piecePos in this case. As the HDFS only provides the append
    // operation
    // TODO change the blockSize type to long instead of int 
    @Override
    public void writePiece(int piecePos, byte[] piece) throws IOException, NoSuchAlgorithmException {
        fdos.write(piece);
        fdos.flush();
    }

}
