#!/bin/bash

source ./config.sh || { eko RED "config.sh not found."; }

# List of remote machines
if [ "$NUM_NODES" == "3" ]; then
    MACHINES=("$VM_USERNAME@$MASTER_IP" "$VM_USERNAME@$WORKER_IP" "$VM_USERNAME@$WORKER2_IP")
else
    MACHINES=("$VM_USERNAME@$MASTER_IP" "$VM_USERNAME@$WORKER_IP")
fi

# Remove the ~/project directory on each machine and then copy the files
for MACHINE in "${MACHINES[@]}"; do
    # Copy files to the ~/project directory using rsync with exclusion of .git directory
    rsync -av --exclude='.git' --exclude='data/soc-redditHyperlinks-body.tsv' $SOURCE_DIR/ $MACHINE:~/project/
done
