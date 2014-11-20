##HDFS File Manager for P2P data transfer
This is just the begining. I am testing and slowly developing a wrapper for a P2P streaming protocol to store the data on HDFS.

Currently, the wrapper allows for user to generate random byte arrays and store it into a user-specified file with customizable block size. Streaming data is written into the HDFS file system and hash of each block of data is calculated and stored along with the data. The interfaces have been made compatible with GVoD, a P2P video streaming protocol. 

###Important packages
Following is a brief description of the packages:

1. mb.hdfs.aux contains the classes used for Logging; Path contruction for HDFS; and unit conversion such as from MB or KB to bytes. 
2. mb.hdfs.core.filemanager contains the File Manager interface and it's implementation.
3. mb.hdfs.core.piecetracker contains the PieceTracker Interface and it's implementation.
4. mb.hdfs.core.storage contains the storage interface, the storage factory and the implementation of storage interface that allows read and write operations on HDFS files
5. mb.hdfs.datagen contain the class to generate random data. This might be replaced in future by the Impostor library. 
6. mb.hdfs.test contains the test program to see the functionality of the wrapper

Message Digest reference:
http://www.javacreed.com/how-to-generate-sha1-hash-value-of-file/