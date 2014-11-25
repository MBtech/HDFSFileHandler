/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.core.filemanager;

import com.sun.media.sound.InvalidDataException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import mb.hdfs.core.storage.Storage;
import mb.hdfs.core.piecetracker.PieceTracker;

/**
 *
 * @author Muhammad Bilal <mubil@kth.se>
 */
public class HDFSFileManager implements FileManager{

    private final Storage file;
    private final PieceTracker pieceTracker;
    private static final int HASHSIZE = 64;
    private final int blockSize;
    private final int pieceSize;
    private final int piecesPerBlock;
    private int currentBlockNumber;
    private final BitSet havePieces;
    private TreeMap<Integer, byte[]> piecesMap = new TreeMap<>();
    private boolean HASHING;
    private final HDFSFileManager hashFileManager;
    private int nPiecesWritten;
    private int blocksWritten;
    private TreeMap<Integer, byte[]> pendingBlocks = new TreeMap<>();
    private TreeMap<Integer, byte[]> pendingBlockHash = new TreeMap<>();
    private String objectType;
    
    
    public HDFSFileManager(Storage file, PieceTracker pieces,String folderName, String fileName, int blockSize, int pieceSize, HDFSFileManager hashFileMngr)
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
        } catch (IOException | NoSuchAlgorithmException ex) {
            Logger.getLogger(HDFSFileManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public void writePiece(int piecePos, byte[] piece) {
        pieceTracker.addPiece(piecePos);
        try {
            writePiece(piecePos, blockSize, piece);
        } catch (IOException | NoSuchAlgorithmException ex) {
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
            throws IOException, NoSuchAlgorithmException {
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
            int discardBlocks = blockPos;
            int ppInBlock = piecePos % piecesPerBlock;

            if (HASHING) {
                System.out.println(objectType + "Reading from index " + blockSize * discardBlocks);
                System.out.println(objectType + "Number of bytes to read " + blockSize);
                readBlock = file.readBlock(discardBlocks);
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
                    readPiece = file.readPiece(piecePos);
                }
            }

            if (HASHING) {
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                md.update(readBlock);
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

    private void writePiece(int piecePos, int blockSize, byte[] piece)
            throws IOException, NoSuchAlgorithmException {
        byte[] hashPiece;
        piecesMap.put(piecePos, piece);
        havePieces.set(piecePos);
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
                        file.writePiece((blocksWritten * piecesPerBlock + j), pendingBlocks.get((blocksWritten * piecesPerBlock + j)));
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
            System.out.println(objectType + "Writing piece number " + piecePos);
            file.writePiece(piecePos, piecesMap.get(piecePos));
            //piecesMap.remove(piecePos);
            nPiecesWritten++;
        }
    }
    
    /**
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException, NoSuchAlgorithmException{
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
                    file.writePiece((blocksWritten * piecesPerBlock + j), pendingBlocks.get((blocksWritten * piecesPerBlock + j)));
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
    }

    private byte[] toByteString(byte[] hash) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hash.length; i++) {
            sb.append(Integer.toString((hash[i] & 0xff) + 0x100, 16).substring(1));
        }
        System.out.println("Hash of written data: " + sb.toString());
        return sb.toString().getBytes();
    }
}
