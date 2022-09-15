# Description 

Zarka is a distributed, leaderless, scalable NoSQL database management system designed to handle large scales of data distributed across numerous networks and machines It was inspired by the architecture of apache cassandra DBMS To utilize the solutions and models that was designed by the cassandra research project authors

# Node level architecture: LSM Tree: 
Using Two approaches : 
# Using commit logs: 
		Pros: 
				* Crash recovery.
				*	Resilient to data loss; If server crashes the data on the ram won’t be lost because it exists in the commit log.
   			* Reliable for users, keep their promises when the server responded with successfully the data remains there on the disk.
		Cons:
				* Extra storage; every data is duplicated in the LSM tree and in the commit log.
				* Overhead because writing at the desk first.
	Trade offs:
				* Fixed the issue of extra storage by making the commit log saves only the data of the maximum size of the memtable and after it reaches it already 					the memtable will be flushed into the disk, Thus the commit log is being erased.
				* Writing in the commit log by appending which is faster than normal writes.
## Using bloom filter:
		Pros:
				* Faster than disk, writing in memory.
        *	In read using bloom filter to test whether an element is in the SStables.
		Cons:
				* If the server crashes all the data in memory will be lost.
				*	No option for recovery from crashes.
				* Not reliable for users if the user made a request then this data
		General trade-off: 
				* Using both approaches together bloom filter and commit log; by writing in the commit log the request is considered done, and in the background 						server start to process it the lsm-tree This makes the response to the user very fast and reliable.

Choice of memtable tree:
Red-black tree:
Pros: Less rotations and faster in insertion.
Cons: less balanced than avl.
Avl tree:
Pros: faster in reading data, Because it’s more balanced.
Cons: complex insertion and slower.
Trade-off: we chose red-black tree because when dealing with this amount of data and insertion then rotation are more expensive in time than having to do some more comparisons in look-ups.
Row cache:
Pros: Helps to reduce the times of lookups in the disk.
Cons: extra storage.
Trade-off: sparing some memory from ram is better than looking up in disk which is slower than memory by 1000.
