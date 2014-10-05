/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.interfaces;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

/**
 *
 * @author mb
 */
public interface Operations {
    
    public void hdfsWriteData(String folderName, String fileName, int blockSize) throws IOException, NoSuchAlgorithmException;
    
    public void hdfsReadData(String folderName, String fileName) throws IOException;
}
