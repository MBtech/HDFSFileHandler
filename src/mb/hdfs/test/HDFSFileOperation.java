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
import java.util.Set;

import mb.hdfs.aux.HashMismatchException;
import mb.hdfs.aux.UnitConversion;
import mb.hdfs.core.filemanager.FileManager;
import mb.hdfs.core.filemanager.HDFSFileManager;
import mb.hdfs.core.filemanager.HDFSHashManager;
import mb.hdfs.core.piecetracker.HDFSPieceTracker;
import mb.hdfs.core.piecetracker.PieceTracker;
import mb.hdfs.datagen.DataGen;

import mb.hdfs.core.storage.HDFSStorageFactory;
import mb.hdfs.core.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Muhammad Bilal <mubil@kth.se>
 */
public class HDFSFileOperation {

    private static MessageDigest md;
    private static final Logger logger = LoggerFactory.getLogger(HDFSFileOperation.class);

    public static byte[] hash() {
        //md.update(readBlock); //use if hashing is on per block basis
        byte[] hashPiece = md.digest();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hashPiece.length; i++) {
            sb.append(Integer.toString((hashPiece[i] & 0xff) + 0x100, 16).substring(1));
        }
        System.out.println(sb.toString());
        return sb.toString().getBytes();
    }

    /**
     * @param args the command line arguments
     * @throws java.security.NoSuchAlgorithmException
     * @throws java.io.IOException
     */
    public static void main(String[] args)
            throws NoSuchAlgorithmException, IOException, HashMismatchException, InterruptedException {
        //Testing Operations of HDFSStorage
        //NOTE: The write operations need to be close before read can be done.

        Storage hashStorage = HDFSStorageFactory.getExistingFile("MyTestHashFolder",
                "MyTestFile", UnitConversion.mbToBytes(1), 64, null);
        PieceTracker hashPieceTracker = new HDFSPieceTracker(4);
        FileManager hashFileManager = new HDFSHashManager(hashStorage,
                hashPieceTracker, "MyTestHashFolder", "MyTestFile", UnitConversion.mbToBytes(1), 64);

        Storage s = HDFSStorageFactory.getExistingFile("MyTestFolder", "MyTestFile",
                UnitConversion.mbToBytes(1), UnitConversion.kbToBytes(256), hashFileManager);
        PieceTracker p = new HDFSPieceTracker(16);
        FileManager dataFileManager = new HDFSFileManager(s, p, "MyTestFolder",
                "MyTestFile", UnitConversion.mbToBytes(1), UnitConversion.kbToBytes(256), hashFileManager, hashPieceTracker);

        List<byte[]> dataPiece = new ArrayList<>();
        List<byte[]> hashPiece = new ArrayList<>();
        for (int j = 0; j < 4; j++) {
            md = MessageDigest.getInstance("SHA-256");
            for (int i = 0; i < 4; i++) {
                dataPiece.add(j * 4 + i, new DataGen().randDataGen(UnitConversion.kbToBytes(256)));

                md.update(dataPiece.get(j * 4 + i));
            }
            hashPiece.add(j, hash());
        }
        Set<Integer> hashPieceSet;
        Set<Integer> dataPieceSet;
        //If pieces requested at any time are not equal to block size. The problem will arise
        //when hashes don't match and suppose only one block is missing in the middle of blocks
        // that have been received and verified. When next contiguous pieces are asked again
        // otherwise extra data might be downloaded and appended which would mess everthing up
        hashPieceSet = hashPieceTracker.nextPiecesNeeded(1, hashPieceTracker.contiguousStart());
        dataPieceSet = p.nextPiecesNeeded(4, hashPieceTracker.contiguousStart());
        for (Integer i : hashPieceSet) {
            hashFileManager.writePiece(i, hashPiece.get(i));
        }
        logger.info("Hashes written. Written data pieces now");

        dataPieceSet.stream().forEach((i) -> {
            dataFileManager.writePiece(i, dataPiece.get(i));
        });

        dataFileManager.isComplete();
        hashFileManager.isComplete();

        hashPieceSet = hashPieceTracker.nextPiecesNeeded(1, hashPieceTracker.contiguousStart());
        dataPieceSet = p.nextPiecesNeeded(4, hashPieceTracker.contiguousStart());
        for (Integer i : hashPieceSet) {
            hashFileManager.writePiece(i, hashPiece.get(0));
        }
        logger.info("Hashes written. Written data pieces now");
        dataPieceSet.stream().forEach((i) -> {
            dataFileManager.writePiece(i, dataPiece.get(i));
        });

        while (!hashFileManager.isComplete() && !dataFileManager.isComplete()) {
            //System.out.println(hashPieceSet);
            //System.out.println(dataPieceSet);
            hashPieceSet = hashPieceTracker.nextPiecesNeeded(1, hashPieceTracker.contiguousStart());
            dataPieceSet = p.nextPiecesNeeded(4, hashPieceTracker.contiguousStart());
            if (!hashPieceSet.isEmpty()) {
                for (Integer i : hashPieceSet) {
                    hashFileManager.writePiece(i, hashPiece.get(i));
                }
            }
            logger.info("Hashes written. Written data pieces now: In loop");
            if (!dataPieceSet.isEmpty()) {
                dataPieceSet.stream().forEach((i) -> {
                    dataFileManager.writePiece(i, dataPiece.get(i));
                });
            }
            Thread.sleep(1000);
            dataFileManager.isComplete();
            hashFileManager.isComplete();
        }
        
        logger.info("Calling close function");

        logger.info("Start Reading");
        dataFileManager.readPiece(4);
        logger.info("Reading done");
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
