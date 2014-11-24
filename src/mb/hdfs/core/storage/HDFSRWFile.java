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

    private final String fileName, folderName;
    private final int blockSize;
    private final int pieceSize;
    private final int piecesPerBlock;
    private int currentBlockNumber;
    private BitSet havePieces;
    private TreeMap<Integer, byte[]> piecesMap = new TreeMap<Integer, byte[]>();
    private boolean HASHING;
    private FSDataOutputStream fdos;
    private HDFSFileManager hashFileManager;
    private int nPiecesWritten;
    private int blocksWritten;
    private int nPendingBlocks;
    private TreeMap<Integer, byte[]> pendingBlocks = new TreeMap<Integer, byte[]>();
    private TreeMap<Integer, byte[]> pendingBlockHash = new TreeMap<Integer, byte[]>();
    private String objectType;
    private final FileSystem hdfs;
    private Path P;

    public HDFSRWFile(String folderName, String fileName, int blockSize, int pieceSize, HDFSFileManager hashFileMngr)
            throws IOException {
        this.fileName = fileName;
        this.folderName = folderName;
        this.blockSize = blockSize;
        this.pieceSize = pieceSize;
        this.piecesPerBlock = (int) blockSize / pieceSize;
        havePieces = new BitSet();
        currentBlockNumber = 0;
        nPiecesWritten = 0;
        blocksWritten = 0;
        nPendingBlocks = 0;
        hdfs = FileSystem.get(new Configuration());
        P = PathConstruction.CreatePathAndFile(hdfs, folderName, fileName, true);
        fdos = hdfs.create(P, true, blockSize, (short) 1, blockSize);
        //this.close();
        HASHING = false;
        this.objectType = "Hash Manager: ";
        if (hashFileMngr != null) {
            this.objectType = "Data Manager: ";
            this.HASHING = true;
        }
        hashFileManager = hashFileMngr;

        if (blockSize % pieceSize != 0) {
            throw new IllegalArgumentException("Illegal pieceSize: Size should be multiple of blockSize");
        }
    }

    /**
     * Close the instance of Piece Operation Class
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        byte[] hashPiece;
        byte[] hashCal;
        int npending = pendingBlockHash.size();
        for (int i = 0; i < npending; i++) {
            System.out.println(objectType + "Number of pending blocks " + pendingBlockHash.size());
            System.out.println(objectType + "Read hash piece number " + (blocksWritten));
            hashPiece = hashFileManager.readPiece(blocksWritten);
            hashCal = pendingBlockHash.get(blocksWritten);
            System.out.write(toByteString(hashCal));
            System.out.println();
            System.out.write(hashPiece);
            if (Arrays.equals(toByteString(hashCal), hashPiece)) {
                System.out.println(objectType + "Match Successful: Data received is correct");
                System.out.println(objectType + "Writing contiguous pieces to hdfs");
                for (int j = 0; j < piecesPerBlock; j++) {
                    System.out.println(objectType + "Writing piece number " + (blocksWritten * piecesPerBlock + j));
                    fdos.write(pendingBlocks.get((blocksWritten * piecesPerBlock + j)));
                    fdos.flush();
                }
                pendingBlockHash.remove(blocksWritten);
                for (int j = 0; j < piecesPerBlock; j++) {
                    pendingBlocks.remove(blocksWritten * piecesPerBlock + j);
                }
                //Delete the pieces written to keep the size of pieceMap small      
                blocksWritten++;
            } else {
                //Change this exception
                throw new InvalidDataException("The hash results do no match");
            }
        }

        //this.fhos.close();
        //hdfs.close();
    }

    public void open() throws IOException {
        //Path P = PathConstruction.CreatePathAndFile(hdfs, folderName, fileName, false);
        fdos = hdfs.append(P);

    }

    private byte[] toByteString(byte[] hash) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hash.length; i++) {
            sb.append(Integer.toString((hash[i] & 0xff) + 0x100, 16).substring(1));
        }
        System.out.println("Hash of written data: " + sb.toString());
        return sb.toString().getBytes();
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
    private byte[] readPiece(FileSystem hdfs, Path filePath, int piecePos, int blockSize)
            throws IOException, NoSuchAlgorithmException {
        //If the requested piece is the one that we  have already written
        if (nPiecesWritten >= piecePos) {
            // TODO: Should these be moved because hdfs open is being called with every read operation.
            FSDataInputStream fdis = new FSDataInputStream(hdfs.open(filePath));

            byte[] readPiece = new byte[this.pieceSize];
            byte[] readBlock = new byte[blockSize];
            byte[] hashByte = new byte[64];
            byte[] readHashByte;
            //Blocks to be discarded
            int blockPos = (int) Math.ceil(piecePos / piecesPerBlock);
            System.out.println(objectType + "No. of pieces Per block is " + piecesPerBlock);
            System.out.println(objectType + "Block position of concern is " + blockPos);
            int discardBlocks = blockPos;
            int ppInBlock = piecePos % piecesPerBlock;
            System.out.println(objectType + "Piece position in block is " + ppInBlock);
            //System.out.println(fdis.available());

            if (HASHING) {
                System.out.println(objectType + "Reading from index " + blockSize * discardBlocks);
                System.out.println(objectType + "Number of bytes to read " + blockSize);
                fdis.readFully(blockSize * discardBlocks, readBlock, 0, blockSize);

                System.out.println(objectType + "Piece Size is " + pieceSize + " and start index is " + ppInBlock * pieceSize);
                //Get the piece of interest from the block of interest
                System.arraycopy(readBlock, ppInBlock * pieceSize, readPiece, 0, pieceSize);
            } else {
                if (piecesMap.containsKey(piecePos)) {
                    System.out.println(objectType + "The piece is in the pending queue");
                    readPiece = piecesMap.get(piecePos);
                } else {
                    System.out.println(objectType + "Reading from index " + pieceSize * piecePos);
                    System.out.println(objectType + "Number of bytes to read " + pieceSize);
                    //System.out.println(fdis.available());
                    fdis.readFully(pieceSize * piecePos, readPiece, 0, pieceSize);
                }
            }

            if (HASHING) {
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                md.update(readBlock);
                //md.update(readBlock); //use if hashing is on per block basis
                readHashByte = md.digest();
                hashByte = hashFileManager.readPiece(blockPos);
                System.out.write(toByteString(readHashByte));
                System.out.println();
                System.out.write(hashByte); //Correct
                if (Arrays.equals(toByteString(readHashByte), hashByte)) {
                    System.out.println("Match Successful");
                    return readPiece;
                } else {
                    //Change this exception
                    throw new InvalidDataException("The hash results do no match");
                }
            } else {
                System.out.println("Returning read piece");
                return readPiece;
            }
        }
        return null; //Should return an exception
    }

    private void writePiece(FileSystem hdfs, Path filePath, int piecePos, int blockSize, byte[] piece)
            throws IOException, NoSuchAlgorithmException {
        //this.open();
        //System.out.println("Size of writeArray " + writeArray.length);
        byte[] hashPiece;

        piecesMap.put(piecePos, piece);
        havePieces.set(piecePos);
        //System.out.println(havePieces);

        int ncpieces = havePieces.nextClearBit(0);
        System.out.println(objectType + "Number of pieces written " + nPiecesWritten);
        System.out.println(objectType + "Number of contiguous pieces " + (ncpieces - nPiecesWritten));
        if (HASHING) {
            nPiecesWritten = currentBlockNumber * piecesPerBlock;
            if (ncpieces - nPiecesWritten == piecesPerBlock) {

                System.out.println(objectType + "Calculating hashes for contiguous pieces");
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                for (int i = nPiecesWritten; i < ncpieces; i++) {
                    //System.out.println("Writing piece number " + i);
                    md.update(piecesMap.get(i));
                }

                byte[] hashCal = md.digest();
                int blockNumber = (int) piecePos / piecesPerBlock;
                for (int i = nPiecesWritten; i < ncpieces; i++) {
                    pendingBlocks.put(i, piecesMap.get(i));
                }
                pendingBlockHash.put(blockNumber, hashCal);
                for (int j = nPiecesWritten; j < ncpieces; j++) {
                    piecesMap.remove(j);
                }

                currentBlockNumber++;
            }
            int writeablePending = hashFileManager.contiguousStart() - blocksWritten;
            System.out.println(objectType + "Pending number of blocks " + writeablePending);
            System.out.println(objectType + "Number of blocks in pending list " + pendingBlockHash.size());
            for (int i = 0; i < writeablePending; i++) {
                byte[] hashCal;
                System.out.println(objectType + "Read hash piece number " + (blocksWritten));
                hashPiece = hashFileManager.readPiece(blocksWritten);
                System.out.println(pendingBlockHash);
                hashCal = pendingBlockHash.get(blocksWritten);
                System.out.write(toByteString(hashCal));
                System.out.println();
                System.out.write(hashPiece);
                if (Arrays.equals(toByteString(hashCal), hashPiece)) {
                    System.out.println(objectType + "Match Successful: Data received is correct");
                    System.out.println(objectType + "Writing contiguous pieces to hdfs");
                    for (int j = 0; j < piecesPerBlock; j++) {
                        System.out.println(objectType + "Writing piece number " + (blocksWritten * piecesPerBlock + j));
                        fdos.write(pendingBlocks.get((blocksWritten * piecesPerBlock + j)));
                        fdos.flush();
                    }
                    System.out.println("Removing pending hash blocks");
                    pendingBlockHash.remove(blocksWritten);
                    for (int j = 0; j < piecesPerBlock; j++) {
                        pendingBlocks.remove(blocksWritten * piecesPerBlock + j);
                    }
                    //Delete the pieces written to keep the size of pieceMap small      
                    blocksWritten++;
                } else {
                    //Change this exception
                    throw new InvalidDataException("The hash results do no match");
                }
            }

        } else {
            /**
             * for (int i = nPiecesWritten; i < ncpieces; i++) {
             * System.out.println("Writing piece number " + i);
             * fdos.write(piecesMap.get(i)); fdos.flush(); } for (int i =
             * nPiecesWritten; i < ncpieces; i++) { piecesMap.remove(i); }
             * currentBlockNumber++;
             *
             */
            System.out.println(objectType + "Writing piece number " + piecePos);
            fdos.write(piecesMap.get(piecePos));
            fdos.flush();
            //piecesMap.remove(piecePos);
            nPiecesWritten++;
        }
        //this.close();
    }

    //TODO Change type of blockSize to long
    @Override
    public byte[] readPiece(int piecePos) throws IOException, NoSuchAlgorithmException {
        //FileSystem hdfs = FileSystem.get(new Configuration());

        Path filePath = PathConstruction.CreateReadPath(hdfs, folderName, fileName);

        //blockSize = (int)hdfs.getFileStatus(filePath).getBlockSize();
        return readPiece(hdfs, filePath, piecePos, blockSize);
    }

    //TODO There is no need for the piecePos in this case. As the HDFS only provides the append
    // operation
    // TODO change the blockSize type to long instead of int 
    @Override
    public void writePiece(int piecePos, byte[] piece) throws IOException, NoSuchAlgorithmException {
        //FileSystem hdfs = FileSystem.get(new Configuration());

        Path filePath = PathConstruction.CreatePathAndFile(hdfs, folderName, fileName, false);

        //blockSize = (int)hdfs.getFileStatus(filePath).getBlockSize();
        writePiece(hdfs, filePath, piecePos, blockSize, piece);
    }

}
