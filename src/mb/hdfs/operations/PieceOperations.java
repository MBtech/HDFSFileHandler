/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.operations;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

/**
 *
 * @author mb
 */
public interface PieceOperations {
    
    public byte[] readPiece(int piecePos) throws IOException, NoSuchAlgorithmException;
    
    public void writePiece(int piecePos, byte[] piece) throws IOException, NoSuchAlgorithmException;
}
