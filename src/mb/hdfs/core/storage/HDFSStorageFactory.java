/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.core.storage;

import java.io.IOException;
import mb.hdfs.core.filemanager.FileManager;

/**
 *
 * @author Muhammad Bilal <mubil@kth.se>
 */
public class HDFSStorageFactory {
    public static Storage getExistingFile(String folderName, String fileName, int blockSize, int pieceSize, FileManager hashFileManager) throws IOException {
        //File file = new File(pathname);
        return new HDFSRWFile(folderName, fileName, blockSize, pieceSize, hashFileManager);
    }
    
    
    public static Storage getEmptyFile(String folderName, String fileName, int blockSize, int pieceSize) throws IOException {
        /**File file = new File(pathname);
        if (!file.createNewFile()) {
            throw new IOException("Could not create file " + pathname);
        }
        **/
        return new HDFSRWFile(folderName, fileName, blockSize, pieceSize,null);
    }
}
