#!/bin/bash

#### variáveis de ambiente ####
#HADOOP_BASE=/usr/local/Cellar/hadoop/1.2.1/libexec
HFS_BASE_DIR=/user/luiz/orto
HFS_INPUT_DIR=$HFS_BASE_DIR/input
HFS_OUTPUT_DIR=$HFS_BASE_DIR/output

# Compila o programa
ant

# Limpa o diretorio de saida no HFS
$HADOOP/bin/hadoop dfs -rmr $HFS_OUTPUT_DIR

# Executa o experimento 01
$HADOOP/bin/hadoop jar dist/bigdata-images.jar br.edu.ufam.icomp.exp01.FeatureExtractor $HFS_INPUT_DIR/exp01 $HFS_OUTPUT_DIR/exp01

# Executa o experimento 02
$HADOOP/bin/hadoop jar dist/bigdata-images.jar br.edu.ufam.icomp.exp02.SplitTransformer $HFS_INPUT_DIR/exp02 $HFS_OUTPUT_DIR/exp02

# Executa o experimento 03
$HADOOP/bin/hadoop jar dist/bigdata-images.jar br.edu.ufam.icomp.exp03.SimilarityFinder $HFS_INPUT_DIR/exp01 $HFS_OUTPUT_DIR/exp03

