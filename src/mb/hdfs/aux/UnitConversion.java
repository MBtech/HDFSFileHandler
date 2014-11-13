/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.aux;

/**
 *
 * @author Muhammad Bilal <mubil@kth.se>
 */
public class UnitConversion {
    public static int mbToBytes(int mbBlockSize){
        return mbBlockSize*1024*1024;
    }
    
    public static int kbToBytes(int kbBlockSize){
        return kbBlockSize*1024;
    }
}
