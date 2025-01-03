#!/bin/bash

source ./config.sh || { eko RED "config.sh not found."; }

sudo apt update

# check if python3 and pip is installed
if ! command -v python3 &> /dev/null
then
    eko CYAN "python3 could not be found. Installing ..."
    sudo apt install python3 -y
    eko GREEN "python3 installed successfully"
fi

if ! command -v pip &> /dev/null
then
    eko CYAN "pip could not be found. Installing ..."
    sudo apt install python3-pip -y
    eko GREEN "pip installed successfully"
fi

if ! command -v ray &> /dev/null
then
    eko CYAN "ray could not be found. Installing ..."
    pip install ray[default] --break-system-packages
    echo 'export PATH=$HOME/.local/bin:$PATH' >> ~/.bashrc
    source ~/.bashrc
    eko GREEN "ray installed successfully. Restart your terminal or run 'source ~/.bashrc'"
fi