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
    private TreeMap<Integer, byte[]> pendingBlockHash = new TreeMap<>();
    private String objectType;
    private static final Logger logger = LoggerFactory.getLogger(HDFSHashManager.class);

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
    // Don't write the pieces until it's confirmed!!!!!
    @Override
    public void writePiece(int piecePos, byte[] piece) {
        try {
            // This operation has to be deferred to the moment when hashes are matched
            pieceTracker.addPiece(piecePos);
            piecesMap.put(piecePos, piece);
            havePieces.set(piecePos);
            int ncpieces = havePieces.nextClearBit(0);
            //If a previous piece arrive for rewrite
            if(nPiecesWritten>piecePos){
                nPiecesWritten=piecePos;
            }
            logger.debug(objectType + "Number of pieces written " + nPiecesWritten);
            logger.debug(objectType + "Number of contiguous pieces " + (ncpieces - nPiecesWritten));
            
            logger.debug(objectType + "Writing piece number " + piecePos);
            file.writePiece(piecePos, piecesMap.get(piecePos));
            //pieceTracker.addPiece(piecePos);
            //piecesMap.remove(piecePos);

            nPiecesWritten++;
        } catch (IOException | NoSuchAlgorithmException ex) {
            logger.error("Exception occurred", ex);
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

    // Not needed in this case as the pieces are written to HDFS as soon as they arrive
    @Override
    public void close(){

    }
   
}
