#!/bin/bash

# Run this script in master VM

source ./config.sh || { eko RED "config.sh not found."; }

eko CYAN "Setting up environment for hadoop spark"
echo 'spark.eventLog.enabled          true
spark.eventLog.dir              hdfs://o-master:54310/spark.eventLog
spark.history.fs.logDirectory   hdfs://o-master:54310/spark.eventLog
spark.master                    yarn
spark.submit.deployMode         client
spark.driver.memory             1g
spark.executor.memory           1g
spark.executor.cores            1' > $SPARK_HOME/conf/spark-defaults.conf || { eko RED "Failed to write to spark-defaults.conf"; exit 1; }

eko CYAN "Creating spark.eventLog directory in HDFS"
hadoop fs -mkdir /spark.eventLog || { eko RED "Failed to create spark.eventLog directory in HDFS"; exit 1; }

eko CYAN "Starting spark history server"
$SPARK_HOME/sbin/start-history-server.sh || { eko RED "Failed to start spark history server"; exit 1; }

eko CYAN "Adding start-history-server.sh to PATH"
echo 'export PATH=$SPARK_HOME/sbin:$PATH' >> ~/.bashrc || { eko RED "Failed to add start-history-server.sh to PATH"; exit 1; }
