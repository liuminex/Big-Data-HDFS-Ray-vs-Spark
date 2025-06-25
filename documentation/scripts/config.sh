#!/bin/bash

HADOOP_LINK="https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz"
SPARK_LINK="https://dlcdn.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz"
MASTER_PUBLIC_IP="192.168.56.100" # in out local VM setup, public is the same as local
MASTER_IP="192.168.56.104" # local IP
WORKER_IP="192.168.56.105" # local IP
MAX_MEM="1024" # memory to allocate for yarn (must be less than VM memory)

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

