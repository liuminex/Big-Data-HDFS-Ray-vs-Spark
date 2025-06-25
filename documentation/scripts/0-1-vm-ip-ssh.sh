#!/bin/bash

source ./config.sh || { eko RED "config.sh not found."; }

# === Usage Check ===
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <VM1_IP> <VM2_IP>"
    echo "Example: $0 192.168.2.13 192.168.2.14"
    exit 1
fi

VM1_IP="$1"
VM2_IP="$2"
SSH_PUB_KEY_LOC=~/.ssh/id_ed25519
SSH_PUB_KEY="$SSH_PUB_KEY_LOC.pub"

# generate key

echo "[*] Checking for SSH key at $SSH_PUB_KEY"
if [ ! -f "$SSH_PUB_KEY" ]; then
    echo "[!] SSH key not found. Generating one..."
    ssh-keygen -t ed25519 -C "your@email.com" -f "$SSH_PUB_KEY_LOC" || { eko RED "Failed to generate ssh key."; exit 1; }
else
    echo "[✓] SSH key exists."
fi

# copy keys to VMs

echo "[*] Copying SSH key to $VM_USERNAME@$VM1_IP"
ssh-copy-id -i "$SSH_PUB_KEY" "$VM_USERNAME@$VM1_IP" || { eko RED "Failed to copy SSH key to $VM1_IP."; exit 1; }

echo "[*] Copying SSH key to $VM_USERNAME@$VM2_IP"
ssh-copy-id -i "$SSH_PUB_KEY" "$VM_USERNAME@$VM2_IP" || { eko RED "Failed to copy SSH key to $VM2_IP."; exit 1; }

# set static IPs

ssh -t "$VM_USERNAME@$VM1_IP" "echo -e \"network:\n  version: 2\n  renderer: networkd\n  ethernets:\n    enp0s3:\n      dhcp4: no\n      addresses:\n        - $MASTER_IP/24\n      gateway4: 192.168.2.1\n      nameservers:\n        addresses:\n          - 8.8.8.8\n          - 8.8.4.4\" | sudo tee /etc/netplan/01-netcfg.yaml > /dev/null && sudo reboot" || { eko RED "Failed to set static ip to $VM1_IP."; exit 1; }
ssh -t "$VM_USERNAME@$VM2_IP" "echo -e \"network:\n  version: 2\n  renderer: networkd\n  ethernets:\n    enp0s3:\n      dhcp4: no\n      addresses:\n        - $WORKER_IP/24\n      gateway4: 192.168.2.1\n      nameservers:\n        addresses:\n          - 8.8.8.8\n          - 8.8.4.4\" | sudo tee /etc/netplan/01-netcfg.yaml > /dev/null && sudo reboot" || { eko RED "Failed to set static ip to $VM2_IP."; exit 1; }

echo ""
echo "===================="
echo "✅ Now you can connect with:"
echo "ssh $VM_USERNAME@$MASTER_IP  # Master VM"
echo "ssh $VM_USERNAME@$WORKER_IP  # Worker VM"






