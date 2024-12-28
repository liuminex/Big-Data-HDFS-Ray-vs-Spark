#!/bin/bash

# Run this script in each VM to remove everything added by the previous setup script

source ./config.sh || { eko RED "config.sh not found."; }


stop-dfs.sh

# Remove Hadoop and Spark directories and symlinks
eko CYAN "Removing Hadoop and Spark directories and symlinks"
sudo rm -rf /opt/bin/hadoop* /opt/bin/spark* /opt/hadoop* /opt/spark* || { eko RED "Failed to remove Hadoop/Spark directories."; }

# Remove Hadoop and Spark tar files (if they exist)
eko CYAN "Removing Hadoop and Spark tar files"
#rm -f ~/$(basename $HADOOP_LINK) ~/$(basename $SPARK_LINK) || { eko RED "Failed to remove Hadoop/Spark tar files."; }

# Remove data directories
eko CYAN "Removing Hadoop data directories"
sudo rm -rf /opt/data/hadoop* /opt/data/hdfs* || { eko RED "Failed to remove Hadoop data directories."; }
sudo rm -rf /opt/data/hdfs/* /opt/data/hdfs/*

# Remove environment variables from .bashrc
eko CYAN "Removing environment variables from .bashrc"
sed -i '/export JAVA_HOME/d' ~/.bashrc
sed -i '/export HADOOP_HOME/d' ~/.bashrc
sed -i '/export SPARK_HOME/d' ~/.bashrc
sed -i '/export HADOOP_INSTALL/d' ~/.bashrc
sed -i '/export HADOOP_MAPRED_HOME/d' ~/.bashrc
sed -i '/export HADOOP_COMMON_HOME/d' ~/.bashrc
sed -i '/export HADOOP_HDFS_HOME/d' ~/.bashrc
sed -i '/export HADOOP_YARN_HOME/d' ~/.bashrc
sed -i '/export HADOOP_COMMON_LIB_NATIVE_DIR/d' ~/.bashrc
sed -i '/export PATH/d' ~/.bashrc
sed -i '/export HADOOP_CONF_DIR/d' ~/.bashrc
sed -i '/export HADOOP_OPTS/d' ~/.bashrc
sed -i '/export LD_LIBRARY_PATH/d' ~/.bashrc
sed -i '/export PYSPARK_PYTHON/d' ~/.bashrc || { eko RED "Failed to remove environment variables from .bashrc."; }

# Remove Hadoop configurations
eko CYAN "Removing Hadoop configuration files"
rm -f $HADOOP_HOME/etc/hadoop/core-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml $HADOOP_HOME/etc/hadoop/workers $HADOOP_HOME/etc/hadoop/hadoop-env.sh || { eko RED "Failed to remove Hadoop configuration files."; }

# Remove Spark-related configuration if any (specific to the previous script)
eko CYAN "Removing Spark-related configurations"
rm -f $HADOOP_HOME/etc/hadoop/spark-env.sh || { eko RED "Failed to remove Spark configuration file."; }

eko CYAN "Removing JAVA installation"
#sudo apt-get remove --purge default-jdk -y || { eko RED "Failed to remove JAVA."; }

eko CYAN "Finished cleanup"
