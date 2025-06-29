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


### Tutorial

Load data to hdfs:
```bash
data_reddit_100M.csv

hdfs dfs -mkdir /data
hdfs dfs -put data_reddit_100M.csv /data/
```

