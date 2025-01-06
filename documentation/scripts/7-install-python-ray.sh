#!/bin/bash

# Run this script in both VMs

source ./config.sh || { eko RED "config.sh not found."; }

sudo apt update

# check if python3 and pip is installed
if ! command -v python3 &> /dev/null
then
    eko CYAN "python3 could not be found. Installing ..."
    sudo apt install python3 -y || { eko RED "python3 installation failed."; exit 1; }
    eko GREEN "python3 installed successfully"
else
    eko GREEN "python3 is already installed"
fi

if ! command -v pip &> /dev/null
then
    eko CYAN "pip could not be found. Installing ..."
    sudo apt install python3-pip -y || { eko RED "pip installation failed."; exit 1; }
    eko GREEN "pip installed successfully"
else
    eko GREEN "pip is already installed"
fi

if ! command -v ray &> /dev/null
then
    eko CYAN "ray could not be found. Installing ..."
    pip install ray[default] --break-system-packages || pip install ray[default] || { eko RED "ray installation failed."; exit 1; }
    echo 'export PATH=$HOME/.local/bin:$PATH' >> ~/.bashrc
    eko GREEN "ray installed successfully. Restart your terminal or run 'source ~/.bashrc'"
else
    eko GREEN "ray is already installed"
fi

source ~/.bashrc
