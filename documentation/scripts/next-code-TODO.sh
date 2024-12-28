#!/bin/bash

# Run this script in the master VM


source ./config.sh || { eko RED "config.sh not found."; }


eko CYAN "namenode -format"
$HADOOP_HOME/bin/hdfs namenode -format
start-dfs.sh

...


Ελέγχουμε αν λειτουργούν και οι δύο κόμβοι:

firefox http://$MASTER_IP:9870
firefox 
http://o-master:9870 (from host OS) - or use public IP of VM if in okeanos
http://192.168.2.9:9870 (from host OS) - or use public IP of VM if in okeanos

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
        <value>o-master</value>
    </property>
    <property>
        <name>yarn.resourcemanager.webapp.address</name>
        <!--Insert the public IP of your master machine here-->
        <value>$MASTER_PUBLIC_IP:8088</value>
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
        <value>$USER_DIR/opt/spark/yarn/*</value>
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
spark.eventLog.dir              hdfs://o-master:54310/spark.eventLog
spark.history.fs.logDirectory   hdfs://o-master:54310/spark.eventLog
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



