#!/bin/bash

# Run this script as described

source ~/project/documentation/scripts/config.sh || { eko RED "config.sh not found."; }

HEAD_NODE_IP=$MASTER_IP # Master node local IP
HEAD_NODE_PORT="6379" # Default port for Ray head node

# Check the argument passed to the script
if [[ $1 == "master" ]]; then
    ray start \
        --head \
        --node-ip-address=$HEAD_NODE_IP \
        --port=$HEAD_NODE_PORT \
        --dashboard-host="0.0.0.0"
    
    echo "Ray head node started at $HEAD_NODE_IP:$HEAD_NODE_PORT"
    echo "Ray dashboard available at http://$HEAD_NODE_IP:8265"
    
elif [[ $1 == "worker" ]]; then
    
    ray start --address="$HEAD_NODE_IP:$HEAD_NODE_PORT" 
    
    echo "Connected to Ray head node."
else
    echo "Usage:"
    echo "  ./8-configure-ray master   - Starts the Ray head node."
    echo "  ./8-configure-ray worker   - Connects this node to the Ray head node."
    exit 1
fi


