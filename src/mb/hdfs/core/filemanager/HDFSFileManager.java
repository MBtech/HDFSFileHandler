/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.core.filemanager;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Set;
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
    
    
    public HDFSFileManager(Storage file, PieceTracker pieces) {
        this.file = file;
        this.pieceTracker = pieces;
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
            return file.readPiece(piecePos);
        } catch (IOException ex) {
            Logger.getLogger(HDFSFileManager.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(HDFSFileManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public void writePiece(int piecePos, byte[] piece) {
        pieceTracker.addPiece(piecePos);
        try {
            file.writePiece(piecePos, piece);
        } catch (IOException ex) {
            Logger.getLogger(HDFSFileManager.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NoSuchAlgorithmException ex) {
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
}
