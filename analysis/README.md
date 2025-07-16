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

## Triangle Counting
Example parameters for Triangle Counting:
### Spark
```bash
cd ~/project/analysis
spark-submit counting_triangles_spark.py -f data_reddit_100M.csv --num-executors 4
```

### Ray
```bash
cd ~/project/analysis
ray job submit -- python counting_triangles_ray.py -f data_reddit_100M.csv -c 4
```

## PageRank
Example parameters for PageRank:
### Spark
```bash
cd ~/project/analysis
spark-submit pagerank_spark.py -f data_reddit_100M.csv --damping-factor 0.85 --max-iterations 20 --convergence-threshold 1e-6
```

### Ray
```bash
cd ~/project/analysis
ray job submit -- python pagerank_ray.py -f data_reddit_100M.csv --damping-factor 0.85 --max-iterations 20 --convergence-threshold 1e-6 --batch-size 67108864
```

## ETL (Extract, Transform, Load)
Example parameters for ETL benchmarks:
### Spark
```bash
cd ~/project/analysis
spark-submit etl_spark.py -f data_reddit_100M.csv --num-executors 4 --executor-memory 2g --partitions 16
```

### Ray
```bash
cd ~/project/analysis
ray job submit -- python etl_ray.py -f data_reddit_100M.csv -c 4 --memory 8GB
```




