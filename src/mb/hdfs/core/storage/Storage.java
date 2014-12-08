/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.core.storage;

/**
 *
 * @author Muhammad Bilal <mubil@kth.se>
 */
public interface Storage {
    
    public byte[] readPiece(int piecePos);
    public byte [] readBlock(int blockPos);
    public byte[] read(long offset, int length); //Combine readPiece and readBlock into read
    //The problem here is that the length might be smaller than the block size
    public void writePiece(int piecePos, byte[] piece);
    
    
}
