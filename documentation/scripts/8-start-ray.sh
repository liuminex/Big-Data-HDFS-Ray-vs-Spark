#!/bin/bash

# Run this script as described

source ~/project/documentation/scripts/config.sh || { eko RED "config.sh not found."; }

HEAD_NODE_IP=$MASTER_IP # Master node local IP
HEAD_NODE_PORT="6379" # Default port for Ray head node

# Memory configuration to MATCH Spark limits for fair comparison
# Spark uses: 4 executors × 1500MB = 6GB total, 3GB per node
OBJECT_STORE_MEMORY="1200000000"  # 1.2GB object store per node (matches Spark executor memory)
PLASMA_DIRECTORY="/tmp"           # Use /tmp for plasma store
NUM_CPUS="4"                     # 4 CPU cores per VM (matches Spark: 4 executors × 2 cores = 8 total)

# Check the argument passed to the script
if [[ $1 == "master" ]]; then
    echo "Starting Ray head node with Spark-equivalent resource settings..."
    echo "  Object store memory: ${OBJECT_STORE_MEMORY} bytes (~1.2GB, matches Spark executor memory)"
    echo "  CPU cores: ${NUM_CPUS}"
    echo "  Total memory per node: 3GB (matches Spark: 2 executors × 1.5GB per node)"
    echo "  Plasma directory: ${PLASMA_DIRECTORY}"
    
    ray start \
        --head \
        --node-ip-address=$HEAD_NODE_IP \
        --port=$HEAD_NODE_PORT \
        --dashboard-host="0.0.0.0" \
        --object-store-memory=$OBJECT_STORE_MEMORY \
        --plasma-directory=$PLASMA_DIRECTORY \
        --num-cpus=$NUM_CPUS \
        --memory=3000000000 \
        --disable-usage-stats
    
    echo "Ray head node started at $HEAD_NODE_IP:$HEAD_NODE_PORT"
    echo "Ray dashboard available at http://$HEAD_NODE_IP:8265"
    
elif [[ $1 == "worker" ]]; then
    echo "Connecting to Ray head node at $HEAD_NODE_IP:$HEAD_NODE_PORT with Spark-equivalent resource settings..."
    echo "  Object store memory: ${OBJECT_STORE_MEMORY} bytes (~1.2GB, matches Spark executor memory)"
    echo "  CPU cores: ${NUM_CPUS}"
    echo "  Total memory per node: 3GB (matches Spark: 2 executors × 1.5GB per node)"
    echo "  Plasma directory: ${PLASMA_DIRECTORY}"
    
    ray start \
        --address="$HEAD_NODE_IP:$HEAD_NODE_PORT" \
        --object-store-memory=$OBJECT_STORE_MEMORY \
        --plasma-directory=$PLASMA_DIRECTORY \
        --num-cpus=$NUM_CPUS \
        --memory=3000000000 \
        --disable-usage-stats
    
    echo "Connected to Ray head node."
else
    echo "Usage:"
    echo "  ./8-configure-ray master   - Starts the Ray head node."
    echo "  ./8-configure-ray worker   - Connects this node to the Ray head node."
    exit 1
fi


