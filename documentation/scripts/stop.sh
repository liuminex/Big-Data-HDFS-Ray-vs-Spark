#!/usr/bin/env bash

source ~/project/documentation/scripts/config.sh || { eko RED "config.sh not found."; }

set -e

eko CYAN "Stopping all services..."
stop-all.sh

eko CYAN "Stopping DFS..."
stop-dfs.sh

eko CYAN "Stopping YARN..."
stop-yarn.sh

eko CYAN "Removing temporary files..."
rm -f /tmp/hadoop-t-secondarynamenode.pid
rm -f /tmp/hadoop-t-resourcemanager.pid

eko CYAN "Stopping Spark History Server..."
$SPARK_HOME/sbin/stop-history-server.sh

eko CYAN "Stopping Ray..."  
ray stop

