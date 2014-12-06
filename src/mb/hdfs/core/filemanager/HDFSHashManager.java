/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.core.filemanager;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import mb.hdfs.core.piecetracker.PieceTracker;
import mb.hdfs.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private String objectType;
    private static final Logger logger = LoggerFactory.getLogger(HDFSHashManager.class);
    private BitSet verified;

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
        if (nPiecesWritten >= piecePos) {
            // TODO: Should these be moved because hdfs open is being called with every read operation.
            byte[] readPiece = new byte[this.pieceSize];
            //Blocks to be discarded
            int blockPos = (int) Math.ceil(piecePos / piecesPerBlock);
            logger.debug("{0}No. of pieces Per block is {1}", new Object[]{objectType, piecesPerBlock});
            logger.debug("{0}Block position of concern is {1}", new Object[]{objectType, blockPos});

            if (piecesMap.containsKey(piecePos)) {
                logger.info("{0}The piece is in the pending queue", objectType);
                readPiece = piecesMap.get(piecePos);
            } else {
                try {
                    logger.debug(objectType + "Reading from index " + pieceSize * piecePos);
                    logger.debug(objectType + "Number of bytes to read " + pieceSize);
                    readPiece = file.readPiece(piecePos);
                } catch (IOException | NoSuchAlgorithmException ex) {
                    logger.error("Exception occurred", ex);
                }
            }

            logger.info("Returning read piece");
            return readPiece;

        }
        return null; //Should return an exception
    }

    @Override
    public void writePiece(int piecePos, byte[] piece) {
        pieceTracker.addPiece(piecePos);
        byte[] hashPiece = null;
        piecesMap.put(piecePos, piece);
        havePieces.set(piecePos);
        int ncpieces = havePieces.nextClearBit(0);
        //Check the following line of code 
        if(nPiecesWritten>piecePos){ nPiecesWritten=piecePos; }
        logger.debug(objectType + "Number of pieces written " + nPiecesWritten);
        logger.debug(objectType + "Number of contiguous pieces " + (ncpieces - nPiecesWritten));
        nPiecesWritten = currentBlockNumber * piecesPerBlock;
        if (ncpieces - nPiecesWritten == piecesPerBlock) {
            for (int i = nPiecesWritten; i < ncpieces; i++) {
                pendingBlocks.put(i, piecesMap.get(i));
            }
            for (int j = nPiecesWritten; j < ncpieces; j++) {
                piecesMap.remove(j);
            }
            currentBlockNumber++;

        }
        //Check for the pending blocks that are also verified and write them to HDFS
        for(Integer i:pendingBlocks.keySet()){
            if(verified.get(i)){
                try {
                    file.writePiece(i, pendingBlocks.get(i));
                } catch (IOException | NoSuchAlgorithmException ex) {
                    java.util.logging.Logger.getLogger(HDFSHashManager.class.getName()).log(Level.SEVERE, null, ex);
                }
                pendingBlocks.remove(i);
            }
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

    @Override
    public void verifiedPiece(int piecePos) {
        verified.set(piecePos);
    }

}
