#!/bin/bash

# Source directory
SOURCE_DIR=~/Videos/Big-Data-HDFS-Ray-vs-Spark

# List of remote machines
MACHINES=("t@192.168.2.121" "t@192.168.2.122")

# Password for SSH
PASSWORD="1"

# Remove the ~/project directory on each machine and then copy the files
for MACHINE in "${MACHINES[@]}"; do
    # Remove the existing project directory
    #sshpass -p $PASSWORD ssh $MACHINE 'rm -rf ~/project'
    
    # Create a new project directory
    #sshpass -p $PASSWORD ssh $MACHINE 'mkdir ~/project'
    
    # Copy files to the ~/project directory using rsync with exclusion of .git directory
    sshpass -p $PASSWORD rsync -av --exclude='.git' $SOURCE_DIR/ $MACHINE:~/project/
done
