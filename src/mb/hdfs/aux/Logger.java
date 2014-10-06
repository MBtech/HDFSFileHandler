/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.aux;

/**
 *
 * @author mb
 */
public class Logger {
    public static void log(Object logMessage, boolean LOG){
        if (LOG == true){
            System.out.println(logMessage);
        }
    }
}
