#!/bin/bash

source ./config.sh || { eko RED "config.sh not found."; }

stop-dfs.sh

rm -rf /opt/data/hdfs/*
eko CYAN "Old DataNode storage directories removed."

rm -rf /opt/data/hadoop/*
eko CYAN "Hadoop temporary directories cleared."

sudo chown -R $(whoami):$(whoami) /opt/data/hadoop /opt/data/hdfs
eko CYAN "Ownership set for /opt/data/hadoop and /opt/data/hdfs."

eko GREEN "HDFS has been reset. You can now start it again with 'hdfs namenode -format && start-dfs.sh'."
