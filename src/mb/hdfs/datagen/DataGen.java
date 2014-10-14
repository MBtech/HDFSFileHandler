/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.datagen;

/**
 *
 * @author mb
 */
import org.apache.commons.lang3.RandomUtils;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
public class DataGen {
    /**
     * Generate and return a byte array filled with random data
     * @return 
     */
    public byte[] randDataGen(int count){
        //byte[] randbuf = RandomUtils.nextBytes(new Random().nextInt(500000));
        String s = RandomStringUtils.randomAlphanumeric(count);
        //byte [] randbuf = RandomUtils.nextBytes(500000);
        //System.out.println("uuid = " + uuid);
        return s.getBytes();
        //return randbuf;
    }
}
