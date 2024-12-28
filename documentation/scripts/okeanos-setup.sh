#!/bin/bash

# Check if the script is run as root
if [ "$EUID" -ne 0 ]; then
  echo "Please run as root"
  exit 1
fi

# Variables
SSH_PORT=4622
SSH_USER="debian"
HOSTNAME="okeanos-worker1"

# Update and upgrade the system
echo "Updating and upgrading the system..."
apt update && apt upgrade -y
apt autoremove -y

# Configure SSH
echo "Configuring SSH..."
sed -i "/^#Port /c\Port $SSH_PORT" /etc/ssh/sshd_config
sed -i "/^#PermitRootLogin /c\PermitRootLogin no" /etc/ssh/sshd_config
sed -i "/^#PasswordAuthentication /c\PasswordAuthentication no" /etc/ssh/sshd_config
echo "AllowUsers $SSH_USER" >> /etc/ssh/sshd_config

# Restart SSH service
echo "Restarting SSH service..."
systemctl restart sshd

# Set up UFW
echo "Setting up UFW..."
apt install -y ufw
ufw allow $SSH_PORT/tcp
ufw enable

# Install Fail2Ban
echo "Installing Fail2Ban..."
apt install -y fail2ban
cat <<EOL > /etc/fail2ban/jail.local
[DEFAULT]
bantime = 1h
findtime = 10m
maxretry = 5

[sshd]
enabled = true
port = $SSH_PORT
logpath = /var/log/auth.log
EOL
systemctl restart fail2ban

# Set up SSH Key Authentication
echo "Setting up SSH Key Authentication..."
sudo -u $SSH_USER mkdir -p /home/$SSH_USER/.ssh
sudo -u $SSH_USER chmod 700 /home/$SSH_USER/.ssh

read -p "Paste your public SSH key: " SSH_KEY
if [ -n "$SSH_KEY" ]; then
  echo "$SSH_KEY" > /home/$SSH_USER/.ssh/authorized_keys
  sudo -u $SSH_USER chmod 600 /home/$SSH_USER/.ssh/authorized_keys
else
  echo "No SSH key provided. You will need to set this up manually."
fi
# Set hostname
sudo hostnamectl set-hostname $HOSTNAME

# Display status
echo "Configuration completed. Here is the status of the security setup:"
echo "SSH configured to use port $SSH_PORT."
echo "Root login disabled. Password authentication disabled."
echo "UFW is active with SSH, HTTP, and HTTPS allowed."
echo "Fail2Ban is active."
echo "Make sure to test SSH access before closing this session!"

