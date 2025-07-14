#!/bin/bash

HADOOP_LINK="https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz"
SPARK_LINK="https://dlcdn.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz"
MAX_MEM="2048" # memory to allocate for yarn (must be less than VM memory)


MASTER_IP="192.168.56.104"
WORKER_IP="192.168.56.105"
SOURCE_DIR=~/Documents/NTUA/Semesters/9/BigData/Big-Data-HDFS-Ray-vs-Spark
VM_USERNAME="ubuntu"
VM_PASSWORD="ubuntu"

if [ "$(whoami)" == "aa" ] || [ "$(whoami)" == "t" ]; then # setup is jason
    MASTER_IP="192.168.2.121" # local IP
    WORKER_IP="192.168.2.122" # local IP
    SOURCE_DIR=~/Videos/Big-Data-HDFS-Ray-vs-Spark
    VM_USERNAME="t" # username for SSH
    VM_PASSWORD="t" # VM password
    MAX_MEM="1024"
fi


# if whoami == vm username run a sudo echo and automatically provide the password to skip prompts
if [ "$(whoami)" == "$VM_USERNAME" ]; then
    echo "$VM_PASSWORD" | sudo -S echo "Running as $VM_USERNAME, sudo commands will not prompt for password."
fi 


# printing to terminal

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
LIME='\033[0;92m'
YELLOW='\033[0;93m'
NC='\033[0m' # No Color

eko() {
    # eg: eko RED "Hello, World!"
    echo -e "${!1}${2}${NC}"
}


# print config variables:
eko CYAN "Configuration:"
eko BLUE "MASTER_IP: $MASTER_IP"
eko BLUE "WORKER_IP: $WORKER_IP"
eko BLUE "SOURCE_DIR: $SOURCE_DIR"
eko BLUE "VM_USERNAME: $VM_USERNAME"
eko BLUE "VM_PASSWORD: $VM_PASSWORD"
eko BLUE "HADOOP_LINK: $HADOOP_LINK"
eko BLUE "SPARK_LINK: $SPARK_LINK"
eko BLUE "MAX_MEM: $MAX_MEM"

