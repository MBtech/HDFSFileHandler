/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.test;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import mb.hdfs.aux.UnitConversion;
import mb.hdfs.core.filemanager.HDFSFileManager;
import mb.hdfs.core.piecetracker.HDFSPieceTracker;
import mb.hdfs.core.piecetracker.PieceTracker;
import mb.hdfs.datagen.DataGen;
;
import mb.hdfs.core.storage.HDFSStorageFactory;
import mb.hdfs.core.storage.Storage;

/**
 *
 * @author Muhammad Bilal <mubil@kth.se>
 */


public class HDFSFileOperation {

    private static MessageDigest md;

    public static byte[] hash(){
        //md.update(readBlock); //use if hashing is on per block basis
        byte[] hashPiece = md.digest();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hashPiece.length; i++) {
            sb.append(Integer.toString((hashPiece[i] & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString().getBytes();
    }

    /**
     * @param args the command line arguments
     * @throws java.security.NoSuchAlgorithmException
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws NoSuchAlgorithmException, IOException {

        //Testing Operations of HDFSStorage
        //NOTE: The write operations need to be close before read can be done.
        Storage hashStorage = HDFSStorageFactory.getExistingFile("MyTestHashFolder", "MyTestFile", UnitConversion.mbToBytes(1), 64, null);
        PieceTracker hashPieceTracker = new HDFSPieceTracker(12);
        HDFSFileManager hashFileManager = new HDFSFileManager(hashStorage, hashPieceTracker,"MyTestFolder", "MyTestFile", UnitConversion.mbToBytes(1), UnitConversion.kbToBytes(256), null);
        
        Storage s = HDFSStorageFactory.getExistingFile("MyTestFolder", "MyTestFile", UnitConversion.mbToBytes(1), UnitConversion.kbToBytes(256), hashFileManager);
        PieceTracker p = new HDFSPieceTracker(12);
        HDFSFileManager dataFileManager = new HDFSFileManager(s, p, "MyTestFolder", "MyTestFile", UnitConversion.mbToBytes(1), UnitConversion.kbToBytes(256), hashFileManager);

        md = MessageDigest.getInstance("SHA-256");
        List <byte[]> dataPiece = new ArrayList<>();
        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 4; i++) {
                dataPiece.add(j*4+i, new DataGen().randDataGen(UnitConversion.kbToBytes(256)));
                md.update(dataPiece.get(j*4+i));
                dataFileManager.writePiece(j*4+i, dataPiece.get(j*4+i));
            }
            //hash per block basis
            hashFileManager.writePiece(j, hash());
        }
        //PUT THE CLOSE COMMAND
        //s.close();
        dataFileManager.readPiece(1);
        System.out.println("Reading done");
        System.out.println("Closing the file");
        /*
         //Sending out of order packets
         Set n = new LinkedHashSet();
         n.add(12);
         n.add(14);
         n.add(15);
         n.add(13);
         for (Object i : n) {
         dataFileManager.writePiece((int) i, new DataGen().randDataGen(UnitConversion.kbToBytes(256)));

         }

         dataFileManager.readPiece(14);
         System.out.println("Reading done");
         System.out.println("Closing the file");
         */
    }

}
