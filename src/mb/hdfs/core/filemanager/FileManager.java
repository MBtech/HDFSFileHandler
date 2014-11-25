/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.core.filemanager;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

/**
 *
 * @author Muhammad Bilal <mubil@kth.se>
 */
public interface FileManager {
    public boolean isComplete();
    public Set<Integer> nextPiecesNeeded(int n, int startPos);
    public boolean hasPiece(int piecePos);
    public byte[] readPiece(int piecePos);
    public void writePiece(int piecePos, byte[] piece);
    public int contiguousStart();
    public void close() throws IOException, NoSuchAlgorithmException; //Extra function
}
