# Word-Document-Frequency-PRAM-Algorithm
Given a file system and a root directory on Unix, traverse the tree and 
(i) extracted words from each text file, 
(ii) computed the document frequency (DF) of each word (the number of documents (i.e. files) in which that word occurs), and (iii) determined the words with the K highest DF.

Implemented the parallel algorithm using OpenMP in C++.

Measured the performance for different input directories by varying the following parameters:
  maximum depth (4, 16, 64),
  average depth (2, 8, 32),
  average branching factor (1.x, 4, 16, 64, 256),
  total number of files (102, 104, 106, 108)

For each input measured the performance for p = 1, 2, .. 2q where p is the number of cores used.
