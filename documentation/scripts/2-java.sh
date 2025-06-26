#!/bin/bash

# Run this script in both VMs

source ./config.sh || { eko RED "config.sh not found."; }

# Install JAVA
cd || { eko RED "Failed to navigate to home directory."; }
eko CYAN "Installing JAVA"
sudo apt-get update -y
sudo apt install openjdk-11-jdk -y
java -version || { eko RED "JAVA installation verification failed."; }

