/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.core.piecetracker;
import java.util.BitSet;
import java.util.Set;
import java.util.TreeSet;

/**
 *
 * @author Muhammad Bilal <mubil@kth.se>
 */
public class HDFSPieceTracker implements PieceTracker{
    private final BitSet pieces;
    private final int nrPieces;
    
    public HDFSPieceTracker(int nrPieces) {
        this.pieces = new BitSet(nrPieces);
        this.nrPieces = nrPieces;
    }
    
    @Override
    public boolean isComplete() {
        return pieces.nextClearBit(0) == nrPieces;
    }
    
    @Override
    public boolean hasPiece(int piecePos) {
        return pieces.get(piecePos);
    }
    
    /**
     * The IDs of the pieces needed next
     * @param n the number of pieces to fetch
     * @param startPos the start position before which all the pieces have been fetched
     * @return the Set with n Ids of the pieces to be fetched next
     */
    @Override
    public Set<Integer> nextPiecesNeeded(int n, int startPos) {
        Set<Integer> result = new TreeSet<Integer>(); // why TreeSet? It could be linkedHashset
        int nextPos = startPos;
        while(result.size() < n) {
            nextPos = pieces.nextClearBit(nextPos);
            if(nextPos < nrPieces) {
                result.add(nextPos);
                nextPos++;
            } else {
                break;
            }
        }
        return result;
    }
    
    @Override
    public void addPiece(int piecePos) {
        pieces.set(piecePos);
    }
    
    @Override
    public int contiguousStart() {
        int nextClear = pieces.nextClearBit(0);
        //return (nextClear == 0 ? 0 : nextClear-1);
        return (nextClear == 0 ? 0 : nextClear);
    } 
}
