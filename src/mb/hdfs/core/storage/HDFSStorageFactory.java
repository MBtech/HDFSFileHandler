/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.core.storage;

import java.io.IOException;
import mb.hdfs.core.filemanager.FileMngr;

/**
 *
 * @author Muhammad Bilal <mubil@kth.se>
 */
public class HDFSStorageFactory {
    public static Storage getExistingFile(String folderName, String fileName, long fileBlockSize, int blockSize, int pieceSize, FileMngr hashFileManager) throws IOException {
        //File file = new File(pathname);
        return new HDFSRWFile(folderName, fileName, fileBlockSize,blockSize, pieceSize, hashFileManager);
    }
    
    
    public static Storage getEmptyFile(String folderName, String fileName, long fileBlockSize, int blockSize, int pieceSize) throws IOException {
        /**File file = new File(pathname);
        if (!file.createNewFile()) {
            throw new IOException("Could not create file " + pathname);
        }
        **/
        return new HDFSRWFile(folderName, fileName, fileBlockSize,blockSize, pieceSize,null);
    }
}
