##HDFS File Handler Wrapper
This is just the begining. I am testing and slowly developing a wrapper for a P2P streaming protocol to store the data on HDFS.

Currently, the wrapper allows for user to generate random byte arrays and store it into a user-specified file with customizable block size. Streaming data is written into the HDFS file system and hash of each block of data is calculated and stored along with the data. 

###Important packages
The package mb.hdfs.operations contains two different mode of operations at the moment:

1. The streamingOps generates random data and writes it in a streaming fashion to the HDFS without waiting for a full block to be accumulated.
2. The pieceOps allow to read and write individual pieces instead of full file read and writes. pieceOps does not do integrity checks using hashing at the moment. (Coming Soon)

####Note
BlockOps has been removed as much of it's functionality is incooperated in PieceOps

Message Digest reference:
http://www.javacreed.com/how-to-generate-sha1-hash-value-of-file/
