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
import mb.hdfs.aux.HashMismatchException;
import mb.hdfs.aux.PathConstruction;
import mb.hdfs.aux.UnitConversion;
import mb.hdfs.datagen.DataGen;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Muhammad Bilal <mubil@kth.se>
 */
public class FileCreation {

    private static MessageDigest md;
    private static final Logger logger = LoggerFactory.getLogger(HDFSFileOperation.class);

    public static byte[] hash() {
        //md.update(readBlock); //use if hashing is on per block basis
        byte[] hashPiece = md.digest();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hashPiece.length; i++) {
            sb.append(Integer.toString((hashPiece[i] & 0xff) + 0x100, 16).substring(1));
        }
        //System.out.println(sb.toString());
        return sb.toString().getBytes();
    }

    public static void createFile(String[] args)
    throws NoSuchAlgorithmException, IOException, HashMismatchException, InterruptedException{
        //TODO: Is it protected in case of replication??
        // IS THE REPLICATION HANDLED?
        int fileSize = UnitConversion.mbToBytes(Integer.parseInt(args[0]));
        int dataBlockSize = UnitConversion.kbToBytes(Integer.parseInt(args[1]));
        boolean append = Boolean.parseBoolean(args[2]);
        int dataPieceSize = UnitConversion.kbToBytes(16);
        int nDataPieces = fileSize / dataPieceSize;
        long hdfsFileBlockSize = UnitConversion.mbToBytes(1);
        System.out.println(append);
        int piecesPerBlock = dataBlockSize / dataPieceSize;
        int hashPieceSize = 64;
        System.out.println("Pieces per block: " + piecesPerBlock);
        int hashBlockSize = UnitConversion.mbToBytes(1);

        int nHashPieces = nDataPieces / piecesPerBlock;
        List<byte[]> dataPiece = new ArrayList<>();
        List<byte[]> hashPiece = new ArrayList<>();
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem hdfs = FileSystem.get(conf);
        Path P1 = PathConstruction.CreatePathAndFile(hdfs, "SourceFolder", "DataFile", append);
        Path P2 = PathConstruction.CreatePathAndFile(hdfs, "SourceFolder", "HashFile", append);
        FSDataOutputStream fdos1, fdos2;
        if(append){
        fdos1 = hdfs.create(P1, append, dataBlockSize, (short) 1, hdfsFileBlockSize);
        fdos2 = hdfs.create(P2, append, hashBlockSize, (short) 1, hdfsFileBlockSize);
        }
        else{
             fdos1 = hdfs.append(P1);
             fdos2 = hdfs.append(P2);
        }
        for (int j = 0; j < nHashPieces; j++) {
            md = MessageDigest.getInstance("SHA-256");
            for (int i = 0; i < piecesPerBlock; i++) {
                dataPiece.add(j * piecesPerBlock + i,
                        new DataGen().randDataGen(dataPieceSize));
                md.update(dataPiece.get(j * piecesPerBlock + i));
                fdos1.write(dataPiece.get(j * piecesPerBlock + i));
            }
            hashPiece.add(j, hash());
            fdos2.write(hashPiece.get(j));
        }
        fdos2.close();
        fdos1.close();
    }
    /**
     * @param args the command line arguments
     * @throws java.security.NoSuchAlgorithmException
     * @throws java.io.IOException
     * @throws mb.hdfs.aux.HashMismatchException
     * @throws java.lang.InterruptedException
     */
    public static void main(String[] args)
            throws NoSuchAlgorithmException, IOException, HashMismatchException, InterruptedException {
        
        String [] arguments = new String[3];
        arguments[0] = "64";
        arguments[1] = args[1];
        arguments[2] = "true";
        int loopFor = Integer.parseInt(args[0])/64;
        createFile(arguments);
        arguments[2] = "false";
        for(int i = 1; i<loopFor;i++){
            createFile(arguments);
        }
    }

}