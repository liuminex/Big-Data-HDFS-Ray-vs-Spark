## Install and Setup

### Create the machines

To run the project in a local virtual machine cluster, check out [how to create the VMs in your local machine](https://github.com/liuminex/Big-Data-HDFS-Ray-vs-Spark/blob/main/documentation/tutorials/create-vms.md).

### Configuration
Clone the repository in your host machine:
```bash
git clone https://github.com/liuminex/Big-Data-HDFS-Ray-vs-Spark
cd Big-Data-HDFS-Ray-vs-Spark
```

Edit the `config.sh` file in `documentation/scripts/` and set the variables according to your setup (IP addresses, memory, number of nodes).

You can transfer the files to the VMs using `scp` or any other method you prefer. You can use the script provided in `documentation/scripts/transfer-files-to-vms.sh` to help you with that.
We will assume you have placed the files in `~/project` in all VMs.

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
ip a 
```

**First update variables in `config.sh`!**

![Host OS Badge](https://img.shields.io/badge/Host%20OS-4284f5) Setup static IPs for the VMs:
```bash
cd ~/project/documentation/scripts
./0-1-vm-ip-ssh.sh
```
After they finish rebooting:

![Host OS Badge](https://img.shields.io/badge/Host%20OS-4284f5) Setup VΜ hostnames:
```bash
./0-2-vm-hostnames.sh
```

#### Connect to the VMs

![Host OS Badge](https://img.shields.io/badge/Host%20OS-4284f5) Connect to the VM:
```bash
ssh [VM Username]@[Static IP we set before]

# examples:

ssh t@192.168.2.121

ssh debian@192.168.56.104
```

### Install and configure

After you update/get the files:

![All VMs Badge](https://img.shields.io/badge/VM-All-ff5733)
```bash
cd ~/project/documentation/scripts
./1-hosts-ssh.sh
sudo reboot

cd ~/project/documentation/scripts
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

![All VMs Badge](https://img.shields.io/badge/VM-All-ff5733) run command `jps`. You should see these in the master VM:
- `NameNode`
- `DataNode`

and this in the workers VM:
- `DataNode`

Or go to [http://[MASTER IP]:9870](http://o-master:9870) and check if all the expected nodes are live.

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

![Master VM Badge](https://img.shields.io/badge/VM-Master-f59542) `yarn node -list`. You should see two (or three depending on the setup) nodes (wait a bit first!)

Or 
![All VMs Badge](https://img.shields.io/badge/VM-All-ff5733) run command `jps`. In addition to the previous outputs of jps, you should also see these in the master:
- `NodeManager`
- `ResourceManager`
- `SecondaryNameNode`
- `DataNode`

and this in the worker:
- `NodeManager`

Or go to [http://[MASTER IP]:8088/cluster/nodes](http://o-master:8088/cluster/nodes) and check if all the expected nodes are live.

#### Continue setup

![Master VM Badge](https://img.shields.io/badge/VM-Master-f59542)
```bash
./6-spark.sh
source ~/.bashrc
```

#### Confirm history server

![Master VM Badge](https://img.shields.io/badge/VM-Master-f59542) `jps`. Find `HistoryServer`.

Or go to [http://[MASTER IP]:18080](http://o-master:18080).


#### Confirm spark
![Master VM Badge](https://img.shields.io/badge/VM-Master-f59542)

If your MAX_MEM > 1024:
```bash
spark-submit --class org.apache.spark.examples.SparkPi /opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar 100
```
Else:
```bash
spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.executor.memory=512m \
  --conf spark.driver.memory=512m \
  /opt/spark/examples/jars/spark-examples_2.12-3.5.6.jar 100  
```
You must be able to monitor the progress at:
- YARN web application [http://[MASTER IP]:8088](http://o-master:8088)
- History Server [http://[MASTER IP]:18080](http://o-master:18080) after the job finishes

#### Continue setup
![All VMs Badge](https://img.shields.io/badge/VM-All-ff5733)
```bash
./7-install-python-ray.sh
source ~/.bashrc
```
![Master VM Badge](https://img.shields.io/badge/VM-Master-f59542)
```bash
./8-start-ray.sh master
```
![Worker VM Badge](https://img.shields.io/badge/VM-Worker-f5dd42)
```bash
./8-start-ray.sh worker
```

#### Confirm Ray

![Master VM Badge](https://img.shields.io/badge/VM-Master-f59542)
```bash
python3 ./test-ray.py
```
Ray dashboard should be available at [http://[MASTER IP]:8265](http://o-master:8265).