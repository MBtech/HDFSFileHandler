This is just the begining. I am testing and slowly developing a wrapper for a P2P streaming protocol to store the data on HDFS.
Currently, the wrapper allows for user to generate random byte arrays and store it into a user-specified file with customizable block size. Streaming data is written into the HDFS file system and hash of each block of data is calculated and stored along with the data. 
