## Okeanos VMs

Create account at [okeanos-knossos.grnet.gr](https://okeanos-knossos.grnet.gr/home/) (choose Academic Login and sign in with NTUA).

Create cryptographic key (locally):
```bash
ssh-keygen
cat ~/.ssh/id_rsa.pub # public key
cat ~/.ssh/id_rsa # private key
```

## Από τον [οδηγό](https://colab.research.google.com/drive/1pjf3Q6T-Ak2gXzbgoPpvMdfOHd1GqHZG?usp=sharing)

0. Get VMs [...] (see guide)
1. Update the VMs [...] (see guide)

2. Install Apache Spark and Hadoop

### In each VM

Install JAVA
```bash
sudo apt-get update
sudo apt install default-jdk -y
java -version # confirm
```

Install Spark, Hadoop
```bash
mkdir ./opt
mkdir ./opt/bin
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xvzf hadoop-3.3.6.tar.gz
mv hadoop-3.3.6 ./opt/bin
wget https://dlcdn.apache.org/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz
tar -xvzf spark-3.5.2-bin-hadoop3.tgz
mv ./spark-3.5.2-bin-hadoop3 ./opt/bin/
cd ./opt
ln -s ./bin/hadoop-3.3.6/ ./hadoop
ln -s ./bin/spark-3.5.2-bin-hadoop3/ ./spark
cd
rm hadoop-3.3.6.tar.gz
rm spark-3.5.2-bin-hadoop3.tgz
mkdir ~/opt/data
mkdir ~/opt/data/hadoop
mkdir ~/opt/data/hdfs
```

Add the following to `~/.bashrc`:
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64  #Value should match: dirname $(dirname $(readlink -f $(which java)))
export HADOOP_HOME=/home/user/opt/hadoop
export SPARK_HOME=/home/user/opt/spark
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$SPARK_HOME/bin;
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
export LD_LIBRARY_PATH=/home/ubuntu/opt/hadoop/lib/native:$LD_LIBRARY_PATH
export PYSPARK_PYTHON=python3
```
```bash
source ~/.bashrc
nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh
```
add the following:
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

```bash
nano $HADOOP_HOME/etc/hadoop/core-site.xml
```
add the following:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/home/user/opt/data/hadoop</value>
        <description>Parent directory for other temporary directories.</description>
    </property>
    <property>
        <name>fs.defaultFS </name>
        <value>hdfs://okeanos-master:54310</value>
        <description>The name of the default file system. </description>
    </property>
</configuration>
```

```bash
nano $HADOOP_HOME/etc/hadoop/hdfs-site.xml
```
add the following:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
        <description>Default block replication.</description>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/home/user/opt/data/hdfs</value>
    </property>
</configuration>
```

```bash
nano $HADOOP_HOME/etc/hadoop/workers
```
add the following:
```bash
okeanos-master
okeanos-worker
```
Εδώ ο master θα λειτουργεί και ως worker.


### In master VM only

```bash
$HADOOP_HOME/bin/hdfs namenode -format
start-dfs.sh
```

Ελέγχουμε αν λειτουργούν και οι δύο κόμβοι:

`http://[master node public IP]:9870` (θα πρέπει να βλέπουμε 2 διαθέσιμους live nodes)


```bash
nano $HADOOP_HOME/etc/hadoop/yarn-site.xml
```

add the following (<b>Insert the public IP of your master machine</b>):
```xml
<?xml version="1.0"?>
<configuration>
<!-- Site specific YARN configuration properties -->
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>okeanos-master</value>
    </property>
    <property>
        <name>yarn.resourcemanager.webapp.address</name>
        <!--Insert the public IP of your master machine here-->
        <value>83.212.xxx.xxx:8088</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>6144</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>6144</value>
    </property>
    <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>128</value>
    </property>
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
    </property>
   <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle,spark_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
        <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.spark_shuffle.class</name>
        <value>org.apache.spark.network.yarn.YarnShuffleService</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.spark_shuffle.classpath</name>
        <value>/home/user/opt/spark/yarn/*</value>
    </property>
</configuration>
```

```bash
start-yarn.sh
```

Ελέγχουμε αν λειτουργούν και οι δύο κόμβοι:

`http://[master node public IP]:8088/cluster` (θα πρέπει να βλέπουμε 2 διαθέσιμους live nodes)


### Configure Spark
```bash
nano $SPARK_HOME/conf/spark-defaults.conf
```
add the following:
```bash
spark.eventLog.enabled          true
spark.eventLog.dir              hdfs://okeanos-master:54310/spark.eventLog
spark.history.fs.logDirectory   hdfs://okeanos-master:54310/spark.eventLog
spark.master                    yarn
spark.submit.deployMode         client
spark.driver.memory             1g
spark.executor.memory           1g
spark.executor.cores            1
```
```bash
hadoop fs -mkdir /spark.eventLog
$SPARK_HOME/sbin/start-history-server.sh
```

Επιβεβαιώνουμε ότι η διαδικασία έχει επιτύχει προσπελαύνοντας με ένα browser το web interface στην public ip του master: `http://[83.212.xxx.xxx]::18080`

Εκτέλεση παραδείγματος εφαρμογής Spark για επιβεβαίωση ορθότητας:
```bash
spark-submit --class org.apache.spark.examples.SparkPi ~/opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar 100
```

## Useful links
- [http://???:8088](http://83.212.xxx.xxx:8088)
- [http://???::18080](http://83.212.xxx.xxx::18080)



