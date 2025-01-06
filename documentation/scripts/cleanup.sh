#!/bin/bash

# Run this script in each VM to remove everything added by the previous setup script

source ./config.sh || { eko RED "config.sh not found."; }

stop-all.sh
ray stop

#stop-dfs.sh
#stop-yarn.sh

# Remove Hadoop and Spark directories and symlinks
eko CYAN "Removing Hadoop and Spark directories and symlinks"
sudo rm -rf /opt/bin/hadoop* /opt/bin/spark* /opt/hadoop* /opt/spark* || { eko RED "Failed to remove Hadoop/Spark directories."; }
sudo rm -rf /opt/data/hadoop* /opt/data/hadoop/* || { eko RED "Failed to remove Hadoop data directories."; }
sudo rm -rf /opt/data/hdfs/* /opt/data/hdfs*

# Remove Hadoop and Spark tar files (if they exist)
eko CYAN "Removing Hadoop and Spark tar files"
eko YELLOW "Tar files were not removed to save time. Make sure they are not damaged (if they do not exist, it's ok)"
#rm -f $(basename $HADOOP_LINK) $(basename $SPARK_LINK) || { eko RED "Failed to remove Hadoop/Spark tar files."; }

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

# Remove configurations
eko CYAN "Removing configuration files"
rm -f $HADOOP_HOME/etc/hadoop/core-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml $HADOOP_HOME/etc/hadoop/workers $HADOOP_HOME/etc/hadoop/hadoop-env.sh || { eko RED "Failed to remove Hadoop configuration files."; }
rm -f $HADOOP_HOME/etc/hadoop/spark-env.sh || { eko RED "Failed to remove Spark configuration file."; }
rm -f $HADOOP_HOME/etc/hadoop/* || { eko RED "Failed to remove Hadoop configuration files (3)."; }

eko CYAN "Removing JAVA installation"
eko YELLOW "JAVA was not removed to save time. Make sure it is ok"
#sudo apt-get remove --purge default-jdk -y || { eko RED "Failed to remove JAVA."; }

eko CYAN "Finished cleanup"
