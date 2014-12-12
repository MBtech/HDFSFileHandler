/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.core.filemanager;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
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
public class HDFSHashManager implements FileMngr {

    private final Storage file;
    private final PieceTracker pieceTracker;
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
    private Set<Integer> verified = new HashSet<>();

    public HDFSHashManager(Storage file, PieceTracker pieces, String folderName,
            String fileName, int blockSize, int pieceSize)
            throws IOException {
        this.file = file;
        this.pieceTracker = pieces;
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
        //Check for the pending blocks that are also verified and write them to HDFS
        Iterator entries = piecesMap.entrySet().iterator();
        while (entries.hasNext()) {
            Entry e = (Entry) entries.next();
            //System.out.println(verified);
            if (verified.contains((int)e.getKey())) {
                logger.debug("writing piece number "+ e.getKey() + " to disk");
                file.writePiece((int) e.getKey(), (byte[]) e.getValue());
                entries.remove();
            }
        }
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
        // Should add the piece of code that only returns the pieces that have been verified. 
        // and otherwise return an exception should return an exception
            // TODO: Should these be moved because hdfs open is being called with every read operation.
            byte[] readPiece = new byte[this.pieceSize];
            //Blocks to be discarded
            int blockPos = (int) Math.ceil(piecePos / piecesPerBlock);
            logger.debug("{0}No. of pieces Per block is {1}", new Object[]{objectType, piecesPerBlock});
            logger.debug("{0}Block position of concern is {1}", new Object[]{objectType, blockPos});

            if (piecesMap.containsKey(piecePos)) {
                logger.debug("The piece is in the pending queue" + objectType);
                readPiece = piecesMap.get(piecePos);
            } else {
                logger.debug(objectType + "Reading from index " + pieceSize * piecePos);
                logger.debug(objectType + "Number of bytes to read " + pieceSize);
                //readPiece = file.readPiece(piecePos);
                readPiece = file.read(piecePos*pieceSize,pieceSize);
            }

            logger.debug("Returning read piece");
            return readPiece;

    }

    @Override
    public void writePiece(int piecePos, byte[] piece) {
        pieceTracker.addPiece(piecePos);
        piecesMap.put(piecePos, piece);
        havePieces.set(piecePos);
        int ncpieces = havePieces.nextClearBit(0);
        //Check the following line of code 
        if (nPiecesWritten > piecePos) {
            nPiecesWritten = piecePos;
        }
        nPiecesWritten = currentBlockNumber * piecesPerBlock;
        logger.debug(objectType + "Piece number of received data " + piecePos);
        logger.debug(objectType + "Number of pieces written " + nPiecesWritten);
        logger.debug(objectType + "Number of contiguous pieces " + (ncpieces - nPiecesWritten));

        //Check for the pending blocks that are also verified and write them to HDFS
        Iterator entries = piecesMap.entrySet().iterator();
        while (entries.hasNext()) {
            Entry e = (Entry) entries.next();
            //System.out.println(verified);
            if (verified.contains((int)e.getKey())) {
                logger.debug("writing piece number "+ e.getKey() + " to disk");
                file.writePiece((int) e.getKey(), (byte[]) e.getValue());
                entries.remove();
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
        verified.add(piecePos);
    }

}
