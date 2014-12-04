/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.core.filemanager;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import mb.hdfs.aux.HashMismatchException;
import mb.hdfs.core.piecetracker.PieceTracker;
import mb.hdfs.core.storage.Storage;

/**
 *
 * @author Muhammad Bilal <mubil@kth.se>
 */
public class HDFSHashManager implements FileManager {

    private final Storage file;
    private final PieceTracker pieceTracker;
    private final int blockSize;
    private final int pieceSize;
    private final int piecesPerBlock;
    private int currentBlockNumber;
    private final BitSet havePieces;
    private TreeMap<Integer, byte[]> piecesMap = new TreeMap<>();
    private int nPiecesWritten;
    private int blocksWritten;
    private TreeMap<Integer, byte[]> pendingBlocks = new TreeMap<>();
    private TreeMap<Integer, byte[]> pendingBlockHash = new TreeMap<>();
    private String objectType;

    public HDFSHashManager(Storage file, PieceTracker pieces, String folderName, String fileName, int blockSize, int pieceSize)
            throws IOException {
        this.file = file;
        this.pieceTracker = pieces;
        this.blockSize = blockSize;
        this.pieceSize = pieceSize;
        this.piecesPerBlock = (int) blockSize / pieceSize;
        havePieces = new BitSet();
        currentBlockNumber = 0;
        nPiecesWritten = 0;
        blocksWritten = 0;
        this.objectType = "Hash Manager: ";

        if (blockSize % pieceSize != 0) {
            throw new IllegalArgumentException("Illegal pieceSize: Size should be multiple of blockSize");
        }
    }

    @Override
    public boolean isComplete() {
        return pieceTracker.isComplete();
    }

    @Override
    public Set<Integer> nextPiecesNeeded(int n, int startPos) {
        return pieceTracker.nextPiecesNeeded(n, startPos);
    }

    @Override
    public boolean hasPiece(int piecePos) {
        return pieceTracker.hasPiece(piecePos);
    }

    @Override
    public byte[] readPiece(int piecePos) {
        try {
            return readPiece(piecePos, blockSize);
        } catch (IOException | NoSuchAlgorithmException | HashMismatchException ex) {
            Logger.getLogger(HDFSFileManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public void writePiece(int piecePos, byte[] piece) {
        // This operation has to be deferred to the moment when hashes are matched
        pieceTracker.addPiece(piecePos); 
        try {
            writePiece(piecePos, blockSize, piece);
        } catch (IOException | NoSuchAlgorithmException | HashMismatchException ex) {
            Logger.getLogger(HDFSFileManager.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public int contiguousStart() {
        return pieceTracker.contiguousStart();
    }

    @Override
    public String toString() {
        return file.toString();
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
    private byte[] readPiece(int piecePos, int blockSize)
            throws IOException, NoSuchAlgorithmException, HashMismatchException {
        //If the requested piece is the one that we  have already written
        if (nPiecesWritten >= piecePos) {
            // TODO: Should these be moved because hdfs open is being called with every read operation.
            byte[] readPiece = new byte[this.pieceSize];
            byte[] readBlock = new byte[blockSize];
            byte[] hashByte;
            byte[] readHashByte;
            //Blocks to be discarded
            int blockPos = (int) Math.ceil(piecePos / piecesPerBlock);
            System.out.println(objectType + "No. of pieces Per block is " + piecesPerBlock);
            System.out.println(objectType + "Block position of concern is " + blockPos);
            int ppInBlock = piecePos % piecesPerBlock;

            if (piecesMap.containsKey(piecePos)) {
                System.out.println(objectType + "The piece is in the pending queue");
                readPiece = piecesMap.get(piecePos);
            } else {
                System.out.println(objectType + "Reading from index " + pieceSize * piecePos);
                System.out.println(objectType + "Number of bytes to read " + pieceSize);
                readPiece = file.readPiece(piecePos);
            }

            System.out.println("Returning read piece");
            return readPiece;

        }
        return null; //Should return an exception
    }

    private void writePiece(int piecePos, int blockSize, byte[] piece)
            throws IOException, NoSuchAlgorithmException, HashMismatchException {
        byte[] hashPiece;
        piecesMap.put(piecePos, piece);
        havePieces.set(piecePos);
        int ncpieces = havePieces.nextClearBit(0);
        //If a previous piece arrive for rewrite
        if(nPiecesWritten>piecePos){
            nPiecesWritten=piecePos; 
        } 
        System.out.println(objectType + "Number of pieces written " + nPiecesWritten);
        System.out.println(objectType + "Number of contiguous pieces " + (ncpieces - nPiecesWritten));

        System.out.println(objectType + "Writing piece number " + piecePos);
        file.writePiece(piecePos, piecesMap.get(piecePos));
        //pieceTracker.addPiece(piecePos);
        //piecesMap.remove(piecePos);
         
        nPiecesWritten++;
        

    }

    /**
     *
     * @throws IOException
     * @throws java.security.NoSuchAlgorithmException
     * @throws mb.hdfs.aux.HashMismatchException
     */
    @Override
    public void close() throws IOException, NoSuchAlgorithmException, HashMismatchException {

    }
   
}
