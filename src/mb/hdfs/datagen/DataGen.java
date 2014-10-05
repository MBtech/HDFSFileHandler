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
public class DataGen {
    public byte[] randDataGen(){
        byte[] randbuf = RandomUtils.nextBytes(new Random().nextInt(10000));
        return randbuf;
    }
}
