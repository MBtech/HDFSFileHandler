/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.core.storage;

import com.sun.media.sound.InvalidDataException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.TreeMap;
import mb.hdfs.aux.PathConstruction;
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
 * @author mb
 */
public class HDFSRWFile implements Storage {

    private final String fileName, folderName;
    private final int blockSize;
    private final int pieceSize;
    private final int piecesPerBlock;
    private int currentBlockNumber;
    private BitSet havePieces;
    private TreeMap<Integer, byte[]> piecesMap = new TreeMap<Integer, byte[]>();
    private int count;
    private byte[] writeArray;
    private FSDataOutputStream fdos, fhos;

    private final FileSystem hdfs;

    public HDFSRWFile(String folderName, String fileName, int blockSize, int pieceSize)
            throws IOException {
        this.fileName = fileName;
        this.folderName = folderName;
        this.blockSize = blockSize;
        this.pieceSize = pieceSize;
        writeArray = new byte[blockSize];
        this.count = 0;
        this.piecesPerBlock = (int) blockSize / pieceSize;

        havePieces = new BitSet();
        currentBlockNumber = 0;

        hdfs = FileSystem.get(new Configuration());
        Path[] P = PathConstruction.CreatePathAndFile(hdfs, folderName, fileName, true);

        fdos = hdfs.create(P[0], true, blockSize, (short) 1, blockSize);
        fhos = hdfs.create(P[1], true, blockSize, (short) 1, blockSize);
        this.close();
        if (blockSize % pieceSize != 0) {

            throw new IllegalArgumentException("Illegal pieceSize: Size should be multiple of blockSize");
        }
    }

    /**
     * Close the instance of Piece Operation Class
     *
     * @throws IOException
     */
    public void close() throws IOException {
        this.fdos.close();
        this.fhos.close();
        //hdfs.close();

    }

    public void open() throws IOException {
        Path[] P = PathConstruction.CreatePathAndFile(hdfs, folderName, fileName, false);
        fdos = hdfs.append(P[0]);
        fhos = hdfs.append(P[1]);
    }

    private byte[] hash(byte[] dataArray) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(dataArray);
        return md.digest();
    }

    /**
     * Read the piece specified by the piece position
     *
     * @param hdfs File system handle
     * @param filePath File Path
     * @param piecePos The position of the piece
     * @param blockSize Block size
     * @return The bytes read
     * @throws IOException
     */
    private byte[] readPiece(FileSystem hdfs, Path filePath, Path hashFilePath, int piecePos, int blockSize)
            throws IOException, NoSuchAlgorithmException {
        // TODO: Should these be moved because hdfs open is being called with every read operation.
        FSDataInputStream fdis = new FSDataInputStream(hdfs.open(filePath));

        byte[] readPiece = new byte[this.pieceSize];
        byte[] readBlock = new byte[blockSize];
        byte[] hashByte = new byte[64];
        byte[] readHashByte;
        //Blocks to be discarded
        int blockPos = (int) Math.ceil(piecePos / piecesPerBlock);
        System.out.println("No. of pieces Per block is " + piecesPerBlock);
        System.out.println("Block position of concern is " + blockPos);
        int discardBlocks = blockPos; 
        int ppInBlock = piecePos % piecesPerBlock; 
        System.out.println("Piece position in block is " + ppInBlock);
        //System.out.println(fdis.available());

        fdis.readFully(blockSize * discardBlocks, readBlock, 0, blockSize);

        System.out.println("Piece Size is " + pieceSize + " and start index is " + ppInBlock * pieceSize);

        //Get the piece of interest from the block of interest
        System.arraycopy(readBlock, ppInBlock * pieceSize, readPiece, 0, pieceSize);

        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(readPiece);
        //md.update(readBlock); //use if hashing is on per block basis
        readHashByte = md.digest();

        FSDataInputStream fhis = new FSDataInputStream(hdfs.open(hashFilePath));
        fhis.readFully(64*piecePos,hashByte,0,64); //Use in case hashing is for each piece
        //fhis.readFully(64 * discardBlocks, hashByte, 0, 64); //if hashing is per block

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < readHashByte.length; i++) {
            sb.append(Integer.toString((readHashByte[i] & 0xff) + 0x100, 16).substring(1));
        }
        System.out.println("Hash of read data: " + sb.toString());
        
        if (Arrays.equals(sb.toString().getBytes(), hashByte)) {
            System.out.println("Match Successful");
            System.out.println("Exiting read function");
            this.close();
            return readPiece;
        } else {
            //Change this exception
            throw new InvalidDataException("The hash results do no match");
        }
    }

    private void writePiece(FileSystem hdfs, Path filePath, Path hashFilePath, int piecePos, int blockSize, byte[] piece)
            throws IOException, NoSuchAlgorithmException {
        this.open();
        //System.out.println("Size of writeArray " + writeArray.length);
        byte[] hash;

        piecesMap.put(piecePos, piece);
        havePieces.set(piecePos);
        System.out.println(havePieces);
        int nPiecesWritten = currentBlockNumber * piecesPerBlock;
        int ncpieces = havePieces.nextClearBit(nPiecesWritten);
        System.out.println("Number of pieces written "+nPiecesWritten);
        System.out.println("Number of contiguous pieces "+(ncpieces - nPiecesWritten));
        if (ncpieces - nPiecesWritten == piecesPerBlock) {
            System.out.println("Writing contiguous pieces to hdfs");
            for (int i = nPiecesWritten; i <ncpieces; i++) {
                System.out.println("Writing piece number "+ i);
                fdos.write(piecesMap.get(i));
                fdos.flush();
                hash = hash(piecesMap.get(i));

                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < hash.length; j++) {
                    sb.append(Integer.toString((hash[j] & 0xff) + 0x100, 16).substring(1));
                }
                System.out.println("Hex format : " + sb.toString());
                fhos.writeBytes(sb.toString());

                fhos.flush();
            }
            currentBlockNumber++;
        }
        /**
        System.arraycopy(piece, 0, writeArray, count, pieceSize);
        count += pieceSize;
        System.out.println("The current index after increment " + count);

        if (count >= this.blockSize) {
            System.out.println("Cleaning the array");
            fdos.write(writeArray);
            fdos.flush();
            hash = hash(writeArray);

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < hash.length; i++) {
                sb.append(Integer.toString((hash[i] & 0xff) + 0x100, 16).substring(1));
            }
            System.out.println("Hex format : " + sb.toString());
            fhos.writeBytes(sb.toString());

            fhos.flush();

            count = 0;
            writeArray = new byte[blockSize];
        }**/
        this.close();
    }

    //TODO Change type of blockSize to long
    @Override
    public byte[] readPiece(int piecePos) throws IOException, NoSuchAlgorithmException {
        FileSystem hdfs = FileSystem.get(new Configuration());
        Path[] P;
        P = PathConstruction.CreateReadPath(hdfs, folderName, fileName);
        Path filePath = P[0], hashFilePath = P[1];
        //blockSize = (int)hdfs.getFileStatus(filePath).getBlockSize();
        return readPiece(hdfs, filePath, hashFilePath, piecePos, blockSize);
    }

    //TODO There is no need for the piecePos in this case. As the HDFS only provides the append
    // operation
    // TODO change the blockSize type to long instead of int 
    @Override
    public void writePiece(int piecePos, byte[] piece) throws IOException, NoSuchAlgorithmException {
        FileSystem hdfs = FileSystem.get(new Configuration());
        Path[] P;
        P = PathConstruction.CreatePathAndFile(hdfs, folderName, fileName, false);
        Path filePath = P[0], hashFilePath = P[1];
        //blockSize = (int)hdfs.getFileStatus(filePath).getBlockSize();
        writePiece(hdfs, filePath, hashFilePath, piecePos, blockSize, piece);
    }

}