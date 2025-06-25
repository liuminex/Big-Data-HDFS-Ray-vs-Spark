## Documentation

This folder contains tutorials, guides, and installer/setup scripts for the project.


### Create the machines

If you choose to run the project in Okeanos, check out [how to create the VMs in Okeanos](https://github.com/ntua-el20439/Big-Data-HDFS-Ray-vs-Spark/blob/main/documentation/tutorials/create-okeanos.md).
If you choose to run the project in local VMs, check out [how to create the VMs in your local machine](https://github.com/ntua-el20439/Big-Data-HDFS-Ray-vs-Spark/blob/main/documentation/tutorials/create-local.md).


### Access the machines

#### Enable SSH

![All VMs Badge](https://img.shields.io/badge/VM-All-ff5733) Check if enabled: `sudo systemctl status ssh`, if not, enable it:
```bash
sudo apt update
sudo apt install openssh-server
sudo systemctl start ssh
sudo systemctl enable ssh
```

![All VMs Badge](https://img.shields.io/badge/VM-All-ff5733) Find the dynamic IPs of the VMs:
```bash
ip a | grep 192. # find ip (in VM)
```

First update variables in `config.sh`!

![Host OS Badge](https://img.shields.io/badge/Host%20OS-4284f5) Setup static IPs for the VMs:
```bash
cd ~/project/documentation/scripts
./0-1-vm-ip-ssh.sh
```
After they finish rebooting:
![Host OS Badge](https://img.shields.io/badge/Host%20OS-4284f5) Setup Vm hostnames:
```bash
./0-2-vm-hostnames.sh
```

#### Connect to the VMs

##### okeanos
![Host OS Badge](https://img.shields.io/badge/Host%20OS-4284f5)
```bash
ssh debian@snf-*****.ok-kno.grnetcloud.net -p 4622
```

##### local

![Host OS Badge](https://img.shields.io/badge/Host%20OS-4284f5) Connect to the VM:
```bash
ssh username-in-vm@the-static-ip-you-defined-in-config

# examples:

ssh t@192.168.2.121

ssh debian@192.168.56.104
```

### Get/update the files

![Host OS Badge](https://img.shields.io/badge/Host%20OS-4284f5)
```bash
sudo apt-get install sshpass
./transfer-files-to-vms.sh
```

### Install and configure

After you update/get the files:

![All VMs Badge](https://img.shields.io/badge/VM-All-ff5733)
```bash
cd ~/project/documentation/scripts
./1-hosts-ssh.sh
sudo reboot
./2-java.sh
./3-install-hadoop-spark.sh
source ~/.bashrc
./4-configure-hadoop.sh
sudo reboot
```

![Master VM Badge](https://img.shields.io/badge/VM-Master-f59542)
```bash
hdfs namenode -format && start-dfs.sh
```

#### Confirm running hadoop

![All VMs Badge](https://img.shields.io/badge/VM-All-ff5733) run command `jps`. You should see `NameNode` and `DataNode` in the master VM, and `DataNode` in the worker VM.

Or go to [http://o-master (public IP):9870](http://o-master:9870).
Use public IP for okeanos or private IP for local VMs. Example:
[http://192.168.2.121:9870](http://192.168.2.121:9870).
Check if there are two live nodes.

If you don't see the nodes, check the logs:
![All VMs Badge](https://img.shields.io/badge/VM-All-ff5733) `cat /opt/hadoop/logs/hadoop-*.log`

#### Continue setup

![All VMs Badge](https://img.shields.io/badge/VM-All-ff5733)
```bash
./5-yarn-hadoop.sh
```

![Master VM Badge](https://img.shields.io/badge/VM-Master-f59542)
```bash
start-yarn.sh
```

#### Confirm yarn

![Master VM Badge](https://img.shields.io/badge/VM-Master-f59542) `yarn node -list`. You should see two nodes (wait a bit first!)

Or 
![All VMs Badge](https://img.shields.io/badge/VM-All-ff5733) run command `jps`. You should see (inn addition) `NodeManager`, `ResourceManager`, `SecondaryNameNode` and `DataNode` in the master VM, and `NodeManager` in the worker VM.

Or go to [http://o-master (public IP):8088/cluster/nodes](http://o-master:8088/cluster/nodes).
Use public IP for okeanos or private IP for local VMs. Example:
[http://192.168.2.121:8088/cluster/nodes](http://192.168.2.121:8088/cluster/nodes).
Check if there are two nodes.

#### Continue setup

![Master VM Badge](https://img.shields.io/badge/VM-Master-f59542) (η πρωτη εντολη (echo) ιιιιισως να χρειαζεται και στο worker - μαλλον οχι)
```bash
./6-spark.sh
source ~/.bashrc
```

#### Confirm history server

![Master VM Badge](https://img.shields.io/badge/VM-Master-f59542) `jps`. Find `HistoryServer`.

Or go to [http://o-master (public IP):18080](http://o-master:18080).
Use public IP for okeanos or private IP for local VMs. Example:
[http://192.168.2.121:18080](http://192.168.2.121:18080).
Check if there are two nodes.

#### Confirm spark
![Master VM Badge](https://img.shields.io/badge/VM-Master-f59542)
```bash
spark-submit --class org.apache.spark.examples.SparkPi /opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar 100
```

If you used MAX_MEM<=1024, use this command instead:
```bash
spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.executor.memory=512m \
  --conf spark.driver.memory=512m \
  /opt/bin/spark-3.5.4-bin-hadoop3/examples/jars/spark-examples_2.12-3.5.4.jar 100
```
You must be able to monitor the progress at:
- YARN web application (http://83.212.xxx.xxx:8088)
- history server (http://83.212.xxx.xxx::18080) after completion.

#### Continue setup
![All VMs Badge](https://img.shields.io/badge/VM-All-ff5733)
```bash
./7-install-python-ray.sh
source ~/.bashrc
```
![Master VM Badge](https://img.shields.io/badge/VM-Master-f59542)
```bash
./8-start-ray master
```
![Worker VM Badge](https://img.shields.io/badge/VM-Worker-f5dd42)
```bash
./8-start-ray worker
```

#### Confirm Ray

![Master VM Badge](https://img.shields.io/badge/VM-Master-f59542)
```bash
python3 ./test-ray.py
```
Ray dashboard should be available at [http://o-master (public IP):8265](http://o-master:8265) (example: [http://192.168.2.121:8265](http://192.168.2.121:8265))

#### Important

Make sure VM IP addresses have not changed!

### Usage

After successful installation and configuration, after reboot you can start them using:
![Master VM Badge](https://img.shields.io/badge/VM-Master-f59542)
```bash
start-dfs.sh
start-yarn.sh
$SPARK_HOME/sbin/start-history-server.sh
./project/documentation/scripts/8-start-ray master
```

![Worker VM Badge](https://img.shields.io/badge/VM-Worker-f5dd42)
```bash
./project/documentation/scripts/8-start-ray worker
```
