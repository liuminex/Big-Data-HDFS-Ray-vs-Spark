#!/bin/bash

# Run this script in each VM

source ./config.sh || { eko RED "config.sh not found."; }

## hadoop-env.sh
eko CYAN "Adding to hadoop-env.sh"
echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

## spark-env.sh
eko CYAN "Adding to spark-env.sh"
THISFILE=$HADOOP_HOME/etc/hadoop/core-site.xml
> $THISFILE # empty this file
cat <<EOL >> $THISFILE
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/opt/data/hadoop</value>
        <description>Parent directory for other temporary directories.</description>
    </property>
    <property>
        <name>fs.defaultFS </name>
        <value>hdfs://o-master:54310</value>
        <description>The name of the default file system. </description>
    </property>
</configuration>
EOL

## hdfs-site.xml
eko CYAN "Adding to hdfs-site.xml"
THISFILE=$HADOOP_HOME/etc/hadoop/hdfs-site.xml
> $THISFILE # empty this file
cat <<EOL >> $THISFILE
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
        <value>/opt/data/hdfs</value>
    </property>
</configuration>
EOL

## hadoop/workers
eko CYAN "Adding to hadoop/workers"
echo -e "o-master\no-worker\n" > $HADOOP_HOME/etc/hadoop/workers # overwrite file
