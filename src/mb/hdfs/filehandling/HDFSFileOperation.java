/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mb.hdfs.filehandling;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;
import mb.hdfs.datagen.DataGen;
/**
 *
 * @author mb
 */
public class HDFSFileOperation {

    public void hdfsWriteData() throws IOException{
            FileSystem hdfs = FileSystem.get(new Configuration());
            Path HomePath = hdfs.getHomeDirectory();
            Path newFolderPath = new Path("/MyDataFolder");
            newFolderPath = Path.mergePaths(HomePath, newFolderPath);
            
            //Creating a file in HDFS

            Path newFilePath = new Path(newFolderPath+"/newFile.txt");

            hdfs.createNewFile(newFilePath);

     

            //Writing data to a HDFS file

            
            DataGen dg = new DataGen();
            byte[] byt = dg.randDataGen();

            FSDataOutputStream fsOutStream = hdfs.create(newFilePath);

            fsOutStream.write(byt);

            fsOutStream.close();

            System.out.println("Written data to HDFS file.");
    }
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException{
           FileSystem hdfs =FileSystem.get(new Configuration());

            //Print the home directory

            System.out.println("Home folder -" +hdfs.getHomeDirectory());

           

            // Create & Delete Directories

            Path workingDir=hdfs.getWorkingDirectory();

            Path newFolderPath= new Path("/MyDataFolder");

            newFolderPath=Path.mergePaths(workingDir, newFolderPath);

            if(hdfs.exists(newFolderPath))

            {

                  //Delete existing Directory

                  hdfs.delete(newFolderPath, true);

                  System.out.println("Existing Folder Deleted.");

            }

            hdfs.mkdirs(newFolderPath);     //Create new Directory

            System.out.println("Folder Created.");

 

            //Copying File from local to HDFS

            Path localFilePath = new Path("/home/mb/localdata/datafile1.txt");

            Path hdfsFilePath= new Path(newFolderPath+"/dataFile1.txt");

            hdfs.copyFromLocalFile(localFilePath, hdfsFilePath);

            System.out.println("File copied from local to HDFS.");

     

            //Copying File from HDFS to local

            localFilePath=new Path("/home/mb/hdfsdata/datafile1.txt");

            hdfs.copyToLocalFile(hdfsFilePath, localFilePath);

            System.out.println("Files copied from HDFS to local.");

     

           
            Path newFilePath = new Path(newFolderPath+"/newFile.txt");
     

            //Reading data From HDFS File

            System.out.println("Reading from HDFS file.");

            BufferedReader bfr = new BufferedReader(

            new InputStreamReader(hdfs.open(newFilePath)));

            String str = null;

            while ((str = bfr.readLine())!= null)

            {

                  System.out.println(str);

            } 
    }
    
}
