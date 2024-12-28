#!/bin/bash

source ./config.sh || { eko RED "config.sh not found."; }

# Install JAVA
cd || { eko RED "Failed to navigate to home directory."; }
eko CYAN "Installing JAVA"
sudo apt-get update -y
sudo apt install default-jdk -y
java -version || { eko RED "JAVA installation verification failed."; }

