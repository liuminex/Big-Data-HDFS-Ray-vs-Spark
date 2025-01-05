## Documentation

This folder contains tutorials, guides, and installer/setup scripts for the project.


### Create the machines

If you choose to run the project in Okeanos, check out [how to create the VMs in Okeanos](https://github.com/ntua-el20439/Big-Data-HDFS-Ray-vs-Spark/blob/main/documentation/tutorials/create-okeanos.md).
If you choose to run the project in local VMs, check out [how to create the VMs in your local machine](https://github.com/ntua-el20439/Big-Data-HDFS-Ray-vs-Spark/blob/main/documentation/tutorials/create-local.md).


### Access the machines

#### Enable SSH

Check if enabled: `sudo systemctl status ssh`, if not, enable it:
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

First update variables in `config.sh`.

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

Other option to confirm it, is to go to [http://o-master (public IP):9870](http://o-master:9870).
Use public IP for okeanos or private IP for local VMs. Example:
[http://192.168.2.10:9870](http://192.168.2.10:9870).
Check if there are two live nodes.

If you don't see the nodes, check the logs:
```bash
cat /opt/hadoop/logs/hadoop-*.log
```

In the master VM run:
```bash
5-yarn-hadoop.sh
start-yarn.sh
```

Confirm:
```bash
yarn node -list
```
You should see two nodes.

Other option to confirm it, is to go to [http://o-master (public IP):8088/cluster/nodes](http://o-master:8088/cluster/nodes).
Use public IP for okeanos or private IP for local VMs. Example:
[http://192.168.2.10:8088/cluster/nodes](http://192.168.2.10:8088/cluster/nodes).
Check if there are two nodes.


### Important

If VM IP addresses change, you need to update the `config.sh` file and run `1-hosts-ssh.sh` and `5-yarn-hadoop.sh` again.



