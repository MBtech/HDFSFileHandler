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
public class HashMismatchException extends Exception {

    public HashMismatchException(){
        super();
    }

    public HashMismatchException(String message){
        super(message);
    }
}
