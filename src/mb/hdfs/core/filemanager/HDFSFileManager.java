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
import mb.hdfs.aux.HashMismatchException;
import mb.hdfs.core.storage.Storage;
import mb.hdfs.core.piecetracker.PieceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Muhammad Bilal <mubil@kth.se>
 */
public class HDFSFileManager implements FileManager {

    private final Storage file;
    private final PieceTracker pieceTracker;
    private final int blockSize;
    private final int pieceSize;
    private final int piecesPerBlock;
    private int currentBlockNumber;
    private final BitSet havePieces;
    private TreeMap<Integer, byte[]> piecesMap = new TreeMap<>();
    private final FileManager hashFileManager;
    private final PieceTracker hashPieceTracker;
    private int nPiecesWritten;
    private int blocksWritten;
    private TreeMap<Integer, byte[]> pendingBlocks = new TreeMap<>();
    private TreeMap<Integer, byte[]> pendingBlockHash = new TreeMap<>();
    private String objectType;
    private static final Logger logger = LoggerFactory.getLogger(HDFSFileManager.class);

    public HDFSFileManager(Storage file, PieceTracker pieces, String folderName, String fileName, int blockSize, int pieceSize, FileManager hashFileMngr, PieceTracker hashPieceTracker)
            throws IOException {
        this.file = file;
        this.pieceTracker = pieces;
        this.blockSize = blockSize;
        this.pieceSize = pieceSize;
        this.piecesPerBlock = (int) blockSize / pieceSize;
        this.hashPieceTracker = hashPieceTracker;
        havePieces = new BitSet();
        currentBlockNumber = 0;
        nPiecesWritten = 0;
        blocksWritten = 0;
        this.objectType = "Data Manager: ";
        hashFileManager = hashFileMngr;
        if (blockSize % pieceSize != 0) {
            throw new IllegalArgumentException("Illegal pieceSize: Size should be multiple of blockSize");
        }
    }

    @Override
    public boolean isComplete() {
        System.out.println("Is complete function called!");
        byte[] hashPiece;
        int hashBlkAvail = hashFileManager.contiguousStart() - blocksWritten;
        //Iterate over all the pending blocks whose hashes we have
        for (int i = 0; i < hashBlkAvail; i++) {
            byte[] hashCal;
            if (pendingBlockHash.containsKey(blocksWritten)) {
                logger.info("Asking for hash piece number " + blocksWritten);
                hashPiece = hashFileManager.readPiece(blocksWritten);
                hashCal = pendingBlockHash.get(blocksWritten);
                if (Arrays.equals(toByteString(hashCal), hashPiece)) {
                    logger.info(objectType + "Match Successful: Data received is correct");
                    logger.info(objectType + "Writing contiguous pieces to hdfs");
                    for (int j = 0; j < piecesPerBlock; j++) {
                        logger.info(objectType + "Writing piece number " + (blocksWritten * piecesPerBlock + j));
                        file.writePiece((blocksWritten * piecesPerBlock + j), pendingBlocks.get((blocksWritten * piecesPerBlock + j)));
                    }

                    logger.info("Removing pending blocks");
                    pendingBlockHash.remove(blocksWritten);
                    hashFileManager.verifiedPiece(blocksWritten);
                    for (int j = 0; j < piecesPerBlock; j++) {
                        pendingBlocks.remove(blocksWritten * piecesPerBlock + j);

                    }
                    //Delete the pieces written to keep the size of pieceMap small      
                    blocksWritten++;

                } else {
                    // Hash match failed. Discard pieces from queues
                    pendingBlockHash.remove(blocksWritten);
                    hashPieceTracker.clearPiece(blocksWritten);
                    for (int j = 0; j < piecesPerBlock; j++) {
                        pendingBlocks.remove(blocksWritten * piecesPerBlock + j);
                        pieceTracker.clearPiece(blocksWritten * piecesPerBlock + j);
                        havePieces.clear(blocksWritten * piecesPerBlock + j);
                    }
                    currentBlockNumber--;
                    try {
                        System.out.write(toByteString(hashCal));
                        System.out.println("");
                        System.out.write(hashPiece);
                    } catch (IOException ex) {
                        logger.error("Exception occurred", ex);
                    }
                    logger.error("WARN: Hash Mismatch!!");
                    //throw new HashMismatchException("The hash results do no match");
                    break;
                }
            } else {
                break;
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
        //If the requested piece is the one that we  have already written
        if (nPiecesWritten >= piecePos) {
            try {
                // TODO: Should these be moved because hdfs open is being called with every read operation.
                byte[] readPiece = new byte[this.pieceSize];
                byte[] readBlock = new byte[blockSize];
                byte[] hashByte;
                byte[] readHashByte;
                //Blocks to be discarded
                int blockPos = (int) Math.ceil(piecePos / piecesPerBlock);
                int ppInBlock = piecePos % piecesPerBlock;
                //readBlock = file.readBlock(blockPos);
                readBlock = file.read(blockPos*blockSize,blockSize);
                //Debugging logs
                logger.debug("{0}No. of pieces Per block is {1}", new Object[]{objectType, piecesPerBlock});
                logger.debug("{0}Block position of concern is {1}", new Object[]{objectType, blockPos});
                logger.debug("{0}Reading from index {1}", new Object[]{objectType, blockSize * blockPos});
                logger.debug("{0}Piece Size is {1} and start index is {2}", new Object[]{objectType, pieceSize, ppInBlock * pieceSize});

                //Get the piece of interest from the block of interest
                System.arraycopy(readBlock, ppInBlock * pieceSize, readPiece, 0, pieceSize);
                //Calulate hash of whole block
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                md.update(readBlock);
                readHashByte = md.digest();
                hashByte = hashFileManager.readPiece(blockPos);
                //Compare hashes
                if (Arrays.equals(toByteString(readHashByte), hashByte)) {
                    logger.info("Match Successful");
                    return readPiece;
                } else {
                    logger.error("The hash of the data block is: ", hashByte);
                    logger.error("The hash of the block received from hash manager is ", toByteString(readHashByte));
                    throw new HashMismatchException("The hash results do no match");
                    //Handle the exception here as well. Or should we?
                }
            } catch (NoSuchAlgorithmException | HashMismatchException ex) {
                logger.error("Exception occurred", ex);
            }
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
        nPiecesWritten = currentBlockNumber * piecesPerBlock;
        logger.info(objectType + "Piece number received " + piecePos);
        logger.info(objectType + "Number of pieces written " + nPiecesWritten);
        logger.info(objectType + "Number of contiguous pieces " + (ncpieces - nPiecesWritten));

        if (ncpieces - nPiecesWritten == piecesPerBlock) {

            try {
                logger.info(objectType + "Calculating hashes for contiguous pieces");
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
            } catch (NoSuchAlgorithmException ex) {
                logger.error("Exception occurred", ex);
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

    private byte[] toByteString(byte[] hash) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hash.length; i++) {
            sb.append(Integer.toString((hash[i] & 0xff) + 0x100, 16).substring(1));
        }
        //System.out.println("Hash of written data: " + sb.toString());
        return sb.toString().getBytes();
    }

    @Override
    public void verifiedPiece(int peicePos) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
