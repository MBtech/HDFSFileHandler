/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.core.piecetracker;

import java.util.Set;

/**
 *
 * @author mb
 */
public interface PieceTracker {

    public boolean isComplete();
    public Set<Integer> nextPiecesNeeded(int n, int startPos);
    public boolean hasPiece(int piecePos);
    public void addPiece(int piecePos);
    public int contiguousStart();
}
