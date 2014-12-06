/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.core.storage;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

/**
 *
 * @author Muhammad Bilal <mubil@kth.se>
 */
public interface Storage {
    
    public byte[] readPiece(int piecePos) throws IOException, NoSuchAlgorithmException;
    // public byte[] read(long offset, int length); Combine readPiece and readBlock into read
    public void writePiece(int piecePos, byte[] piece) throws IOException, NoSuchAlgorithmException;
    
    public byte [] readBlock(int blockPos) throws IOException; //Extra function
}
