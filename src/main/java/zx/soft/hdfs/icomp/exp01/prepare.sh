#!/bin/bash

#### Environment config ####
HADOOP_BASE=/usr/local/Cellar/hadoop/1.2.1/libexec
HFS_BASE_DIR=/user/luiz/orto
HFS_UNCOMPRESSED_DIR=$HFS_BASE_DIR/uncompressed
HFS_COMPRESSED_DIR=$HFS_BASE_DIR/input
HFS_OUTPUT_DIR=$HFS_BASE_DIR/output
EXTERNAL_DATA=~/Movies/aerial/

# Clean all HDFS directories
echo Cleaning previous data...
$HADOOP_BASE/bin/hadoop dfs -rmr $HFS_UNCOMPRESSED_DIR
$HADOOP_BASE/bin/hadoop dfs -rmr $HFS_COMPRESSED_DIR
$HADOOP_BASE/bin/hadoop dfs -rmr $HFS_OUTPUT_DIR

# Copy all uncompressed images to a temp directory
echo Copying pristine data...
$HADOOP_BASE/bin/hadoop dfs -put $EXTERNAL_DATA/several $HFS_UNCOMPRESSED_DIR/several
$HADOOP_BASE/bin/hadoop dfs -put $EXTERNAL_DATA/huge $HFS_UNCOMPRESSED_DIR/huge

# Take all files into a single SequenceFile
echo Creating SequenceFile...
$HADOOP_BASE/bin/hadoop jar dist/bigdata-images.jar br.edu.ufam.icomp.io.ImageToSequenceFile $HFS_UNCOMPRESSED_DIR/several $HFS_COMPRESSED_DIR/exp01

# Split all images to the split experiment
echo Splitting images...
$HADOOP_BASE/bin/hadoop jar dist/bigdata-images.jar br.edu.ufam.icomp.io.ImageSplitter $HFS_UNCOMPRESSED_DIR/huge $HFS_COMPRESSED_DIR/exp02

# Cleaning temporary data
echo Cleaning temp data...
$HADOOP_BASE/bin/hadoop dfs -rmr $HFS_UNCOMPRESSED_DIR

echo We are ready to go