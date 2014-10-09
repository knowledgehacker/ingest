#!/bin/sh
# By default, the root of hdfs is /user/$USER, output is in "/user/$USER/minglin/hive". But it doesn't exist implicitly, you have to create it explicitly.
export HADOOP_HOME=/opt/hadoop-2.4.1
export HADOOP=$HADOOP_HOME/bin/hadoop
PACKAGE=data-ingester
VERSION=0.1
MAINCLASS=ingest.DataIngester

# run the job
$HADOOP jar target/${PACKAGE}-${VERSION}-jar-with-dependencies.jar $MAIN_CLASS

