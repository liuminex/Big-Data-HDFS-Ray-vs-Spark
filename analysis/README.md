## Analysis

This folder contains scripts that test the performance of Ray and Spark.

TODO:
- PageRank
- Triangle Count
- ETL
- ML:
    - clustering: k-means
    - predictions: xgboost

use different dataset sizes, number of workers
measure time (user & cpu) and memory (peak)


## Requirements

```bash
pip install pandas numpy scikit-learn pyarrow
pip install pyspark
```

### Tutorial

From now on we will not interact with the worker VM anymore. Everything is done in the master.

Load data to hdfs:
```bash

hdfs dfsadmin -report # check status

hdfs dfs -mkdir -p /data # mkdir

hdfs dfs -put ~/project/data/data_reddit_100M.csv /data/ # upload

hdfs dfs -ls /data/ # verify
```

## kmeans

### Spark
```bash
cd ~/project/analysis
spark-submit kmeans_spark.py -f data_reddit_100M.csv
```
#### Ray
```bash
export CLASSPATH="$HADOOP_HOME/etc/hadoop:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*"
```




