## Documentation

This folder contains tutorials, guides, and installer/setup scripts for the project.


### Create the machines

If you choose to run the project in Okeanos, check out [how to create the VMs in Okeanos](https://github.com/ntua-el20439/Big-Data-HDFS-Ray-vs-Spark/blob/main/documentation/tutorials/create-okeanos.md).
If you choose to run the project in local VMs, check out [how to create the VMs in your local machine](https://github.com/ntua-el20439/Big-Data-HDFS-Ray-vs-Spark/blob/main/documentation/tutorials/create-local.md).


### Access the machines

#### Enable SSH

Check if enabled: `sudo systemctl status ssh`, if not enabled, enable it:
```bash
sudo apt update
sudo apt install openssh-server
sudo systemctl start ssh
sudo systemctl enable ssh
```

#### (optional) enable passwordless SSH

>> If you don't have a public key, create one: `ssh-keygen`

In both VMs, add your host's public key (`~/.ssh/id_rsa.pub`) to the VMs' `~/.ssh/authorized_keys` file:
```bash
cat ~/.ssh/id_rsa.pub # host os
echo "ssh-rsa AAAA...your...key...here..." >> ~/.ssh/authorized_keys # VM
```

#### Connect to the VMs

```bash
## okeanos
ssh debian@snf-*****.ok-kno.grnetcloud.net -p 4622

## local
ip a | grep 192.168 # find ip
ssh username-in-vm@192.168.2.9 # example
```


### Change hostnames

In the master VM:
```bash
sudo hostnamectl set-hostname o-master
sudo reboot
```

In the worker VM:
```bash
sudo hostnamectl set-hostname o-worker
sudo reboot
```


### Install and configure

In **both** VMs run:
```bash
1-hosts-ssh.sh
sudo reboot
2-java.sh
3-install-hadoop-spark.sh
4-configure-hadoop.sh
sudo reboot
```

In the master VM run:
```bash
hdfs namenode -format && start-dfs.sh
```

Confirm running hadoop: run in both VMs:
```bash
jps
```
You should see `NameNode` and `DataNode` in the master VM, and `DataNode` in the worker VM.

Other option to confirm it, is to go to [http://o-master:9870](http://o-master:9870)
(example: [http://192.168.2.10:9870](http://192.168.2.10:9870)) and check if there are two live nodes.


