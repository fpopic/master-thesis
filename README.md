# Abstract

The result of this thesis is a distributed recommender system based on the item-item collaborative filtering. 
The recommendation algorithm builds an item-item similarity matrix based on the collaboratively collected data on user-item interactions, for all users in the system. 
The recommendation algorithm supports several similarity measures including a vector normalisation of rows in the matrix. 
Moreover, the recommendation algorithm supports three different distributed matrix multiplication algorithms. 
The entire recommender system source code is written in Scala programming language based on Apache Spark. 
However, the data pre-processing scripts are written in C++ programming language executed in a single-node environment. 
The tests and performance evaluation of the implemented algorithm were executed on a Cloudera cluster using real dataset obtained from the particular case study.
