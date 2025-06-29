#!/bin/bash

# Run this script in both VMs

source ./config.sh || { eko RED "config.sh not found."; }

# Auto variables
HADOOP_FILE=$(basename $HADOOP_LINK)
HADOOP_DIR=${HADOOP_FILE%.tar.gz}
SPARK_FILE=$(basename $SPARK_LINK)
SPARK_DIR=${SPARK_FILE%.tgz}

eko MAGENTA "HADOOP_FILE=$HADOOP_FILE"
eko MAGENTA "HADOOP_DIR=$HADOOP_DIR"
eko MAGENTA "SPARK_FILE=$SPARK_FILE"
eko MAGENTA "SPARK_DIR=$SPARK_DIR"

# Prepare directories
eko CYAN "Creating /opt/bin directories"
sudo mkdir -p /opt/bin || { eko RED "Failed to create /opt/bin."; }

# Install Hadoop
if [ -f "$HOME/$HADOOP_FILE" ]; then
    eko GREEN "Hadoop tar file already exists. Skipping download."
else
    eko CYAN "Downloading Hadoop"
    wget $HADOOP_LINK -P ~/ || { eko RED "Failed to download Hadoop."; }
fi
eko CYAN "Extracting Hadoop"
tar -xvzf $HOME/$HADOOP_FILE -C $HOME > /dev/null || { eko RED "Failed to extract Hadoop."; }
eko CYAN "Moving Hadoop to /opt/bin"
sudo mv $HOME/$HADOOP_DIR /opt/bin || { eko RED "Failed to move Hadoop."; }
eko CYAN "Creating symbolic link for Hadoop"
sudo ln -sf /opt/bin/$HADOOP_DIR /opt/hadoop || { eko RED "Failed to create Hadoop symlink."; }

# Install Spark
if [ -f "$HOME/$SPARK_FILE" ]; then
    eko GREEN "Spark tar file already exists. Skipping download."
else
    eko CYAN "Downloading Spark"
    wget $SPARK_LINK -P ~/ || { eko RED "Failed to download Spark."; }
fi
eko CYAN "Extracting Spark"
tar -xvzf $HOME/$SPARK_FILE -C $HOME > /dev/null || { eko RED "Failed to extract Spark."; }
eko CYAN "Moving Spark to /opt/bin"
sudo mv $HOME/$SPARK_DIR /opt/bin/ || { eko RED "Failed to move Spark."; }
eko CYAN "Creating symbolic link for Spark"
sudo ln -sf /opt/bin/$SPARK_DIR /opt/spark || { eko RED "Failed to create Spark symlink."; }

eko CYAN "Setting up environment directories"
sudo mkdir -p /opt/data/hadoop /opt/data/hdfs || { eko RED "Failed to create data directories."; }

# new
sudo mkdir -p /opt/data/hadoop/dfs/name
sudo mkdir -p /opt/data/hadoop/dfs/namesecondary
sudo mkdir -p /opt/data/hdfs
sudo mkdir -p /opt/data/hadoop/dfs/name/current
sudo chmod -R 700 /opt/data/hadoop/dfs /opt/data/hdfs
sudo chown -R $(whoami):$(whoami) /opt/data/hdfs /opt/bin/hadoop-3.3.6
sudo chmod -R 755 /opt/data/hdfs
sudo chmod -R 755 /opt/bin/hadoop-3.3.6
sudo chmod -R 755 /opt/hadoop
sudo chown -R $(whoami):$(whoami) /opt/data/hadoop/dfs/namesecondary
sudo chmod -R 755 /opt/data/hadoop/dfs/namesecondary


sudo chmod -R 777 /opt




eko CYAN "Finding JAVA_HOME"
JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
eko MAGENTA "[info] JAVA_HOME=$JAVA_HOME"


eko CYAN "Adding environment variables in .bashrc"
HADOOP_HOME=/opt/hadoop
cat << EOF >> ~/.bashrc
export JAVA_HOME=$JAVA_HOME
export HADOOP_HOME=$HADOOP_HOME
export SPARK_HOME=/opt/spark
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=\$HADOOP_HOME/lib/native
export PATH=\$PATH:\$HADOOP_HOME/sbin:\$HADOOP_HOME/bin:\$SPARK_HOME/bin
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export HADOOP_OPTS="-Djava.library.path=\$HADOOP_HOME/lib/native"
export LD_LIBRARY_PATH=\$HADOOP_HOME/lib/native:\$LD_LIBRARY_PATH
export PYSPARK_PYTHON=python3
export CLASSPATH="\$HADOOP_HOME/etc/hadoop:\$HADOOP_HOME/share/hadoop/common/lib/*:\$HADOOP_HOME/share/hadoop/common/*:\$HADOOP_HOME/share/hadoop/hdfs:\$HADOOP_HOME/share/hadoop/hdfs/lib/*:\$HADOOP_HOME/share/hadoop/hdfs/*"
EOF

