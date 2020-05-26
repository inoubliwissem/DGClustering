# DGClustering
This is a distributed  implementation of a structural graph clustering algorithm called SCAN, for more details you can see this paper "SCAN: a structural clustering algorithm for networks
"
# Input data
The input graph must be an adjacency list like the following example:
//VertexID:Neighbour1ID,Neighbour2ID,...,NeighbournID
# The output
 The output result is also a text file, that contains in each line the VertexID and its clusterID, the next example for the output result
VertexID:ClusterID
Note that the -1 and -2 clusterID represent respectively  the outliers and the bridges 
# Compilation 
mvn package
# Execution with a local mode
java -cp target/jarFile [MainClass: DSCANJob] [Input graph: adj.txt] [resultsLocation]
# Execution with a distributed mode
hadoop jar target/jarFile [MainClass: DSCANJob] [Input graph ] [resultsLocation]

