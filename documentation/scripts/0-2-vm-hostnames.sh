#!/bin/bash

source ./config.sh || { eko RED "config.sh not found."; }

ssh-keygen -f ~/.ssh/known_hosts -R "$MASTER_IP"
ssh-keygen -f ~/.ssh/known_hosts -R "$WORKER_IP"
if [ "$NUM_NODES" == "3" ]; then
    ssh-keygen -f ~/.ssh/known_hosts -R "$WORKER2_IP"
fi

# set hostnames
ssh -t "$VM_USERNAME@$MASTER_IP" 'sudo hostnamectl set-hostname o-master && echo "set hostname sucessfully" && sudo reboot' || { eko RED "Failed to set hostname for $MASTER_IP."; exit 1; }
ssh -t "$VM_USERNAME@$WORKER_IP" 'sudo hostnamectl set-hostname o-worker && echo "set hostname sucessfully" && sudo reboot' || { eko RED "Failed to set hostname for $WORKER_IP."; exit 1; }
if [ "$NUM_NODES" == "3" ]; then
    ssh -t "$VM_USERNAME@$WORKER2_IP" 'sudo hostnamectl set-hostname o-worker2 && echo "set hostname sucessfully" && sudo reboot' || { eko RED "Failed to set hostname for $WORKER2_IP."; exit 1; }
fi



