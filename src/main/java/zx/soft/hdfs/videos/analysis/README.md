#Process Video on Hadoop

This is a simple hadoop project which can run a python job on an input file that is taken from HDFS.
In this case, the python job runs opencv code to process videos.
This does not use the full potential of hadoop - For every file in the input HDFS folder a map task would be started and it will download it to the local filesystem and hand it over to the python job for further processing - the result is collected back by the map task.

##Using ProcessVideo

This is a maven project so <code>mvn install</code> will build the jar which you can use in your hadoop cluster


##Requirements
- Maven
- OpenCV 
- Hadoop


