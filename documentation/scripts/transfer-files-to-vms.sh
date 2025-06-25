#!/bin/bash

source ./config.sh || { eko RED "config.sh not found."; }

# List of remote machines
MACHINES=("$VM_USERNAME@$MASTER_IP" "$VM_USERNAME@$WORKER_IP")

# Remove the ~/project directory on each machine and then copy the files
for MACHINE in "${MACHINES[@]}"; do
    # Copy files to the ~/project directory using rsync with exclusion of .git directory
    sshpass -p $VM_PASSWORD rsync -av --exclude='.git' $SOURCE_DIR/ $MACHINE:~/project/
done
