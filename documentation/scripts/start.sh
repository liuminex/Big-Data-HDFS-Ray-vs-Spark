#!/usr/bin/env bash
set -e

# Detect hostname
HOST=$(hostname)

echo "Detected hostname: $HOST"

if [ "$HOST" == "o-master" ]; then
    echo "Starting services on MASTER node..."

    # Start HDFS
    start-dfs.sh

    # Start YARN
    start-yarn.sh

    # Start Spark History Server
    start-history-server.sh

    # Start Ray Master
    ~/project/documentation/scripts/8-start-ray.sh master

    echo "✅ Master services started."

elif [ "$HOST" == "o-worker" ] || [ "$HOST" == "o-worker2" ]; then
    echo "Starting services on WORKER node ($HOST)..."

    # Start Ray Worker
    ~/project/documentation/scripts/8-start-ray.sh worker

    echo "✅ Worker services started."

else
    echo "❌ Unknown hostname: $HOST"
    echo "This script expects 'o-master', 'o-worker', or 'o-worker2'."
    exit 1
fi
