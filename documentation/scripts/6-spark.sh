#!/bin/bash

# Run this script in the master VM

source ./config.sh || { eko RED "config.sh not found."; }

# setup environment for hadoop spark

echo 'spark.eventLog.enabled          true
spark.eventLog.dir              hdfs://o-master:54310/spark.eventLog
spark.history.fs.logDirectory   hdfs://o-master:54310/spark.eventLog
spark.master                    yarn
spark.submit.deployMode         client
spark.driver.memory             1g
spark.executor.memory           1g
spark.executor.cores            1' > $SPARK_HOME/conf/spark-defaults.conf 

hadoop fs -mkdir /spark.eventLog

eko CYAN "Spark setup complete"

echo 'export PATH=$SPARK_HOME/sbin:$PATH' >> ~/.bashrc
source ~/.bashrc

eko GREEN "Added start-history-server.sh to PATH"