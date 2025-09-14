## Analysis

This folder contains scripts that test the performance of Ray and Spark.

TODO:
- PageRank
- Triangle Count
- ETL
- ML/clustering: k-means

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

Run each analysis program like this:

For Spark (example with PageRank):
```bash
cd ~/project/analysis
spark-submit pagerank_spark.py -f data_reddit_5G.csv
```

For Ray (example with PageRank):
```bash
cd ~/project/analysis
python pagerank_ray.py -f data_reddit_5G.csv
```