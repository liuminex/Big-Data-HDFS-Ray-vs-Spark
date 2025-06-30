import sys
import os
import time
import argparse
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from sklearn.cluster import KMeans
from sklearn.metrics import calinski_harabasz_score

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def display_results(config, start_time, end_time, end_time_system, calinski_harabasz_res):
    with open('results.txt', 'w') as f:
        f.write(f"Config: {config}\n")
        f.write(f"Start Time: {start_time}\n")
        f.write(f"End Time: {end_time}\n")
        f.write(f"System Init Time: {end_time_system}\n")
        f.write(f"Calinski-Harabasz Score: {calinski_harabasz_res}\n")


def process_batch_spark(df_batch, config):
    """Process a batch of data and return Calinski-Harabasz score"""
    # Convert categorical columns to numeric codes
    for col_name in df_batch.columns:
        if df_batch[col_name].dtype == 'object':
            df_batch[col_name] = df_batch[col_name].astype('category').cat.codes
        elif pd.api.types.is_datetime64_any_dtype(df_batch[col_name]):
            # Convert datetime to numeric timestamp
            df_batch[col_name] = pd.to_numeric(df_batch[col_name])
        elif not pd.api.types.is_numeric_dtype(df_batch[col_name]):
            # Convert any other non-numeric types to category codes
            df_batch[col_name] = df_batch[col_name].astype('category').cat.codes
    
    # Fill NaNs
    df_batch = df_batch.fillna(-1)
    
    # Ensure all columns are numeric
    df_batch = df_batch.select_dtypes(include=[np.number])
    
    # Check if we have any data left after filtering
    if df_batch.empty or len(df_batch.columns) == 0:
        print("Warning: No numeric columns found in batch")
        return 0.0
    
    data = df_batch.values
    
    # Apply K-means clustering
    kmeans = KMeans(n_clusters=config["n_clusters"], random_state=42)
    kmeans.fit(data)
    
    # Calculate Calinski-Harabasz score
    return calinski_harabasz_score(data, kmeans.labels_)


def distributed_kmeans_spark(spark, config):
    """Main distributed K-means function using Spark"""
    # Read CSV file from HDFS
    hdfs_path = f"hdfs://o-master:54310/data/{config['datafile']}"
    
    # Read the entire CSV file
    df_spark = spark.read.option("header", "true").option("inferSchema", "true").csv(hdfs_path)
    
    # Convert to Pandas DataFrame for processing (in real scenarios, you'd want to keep it as Spark DataFrame)
    df = df_spark.toPandas()
    
    # Process data in batches
    batch_size_rows = config["batch_size_rows"]
    scores = []
    
    num_batches = (len(df) + batch_size_rows - 1) // batch_size_rows
    
    for i in range(num_batches):
        start_idx = i * batch_size_rows
        end_idx = min((i + 1) * batch_size_rows, len(df))
        batch = df.iloc[start_idx:end_idx].copy()
        
        if len(batch) > config["n_clusters"]:  # Need enough samples for clustering
            score = process_batch_spark(batch, config)
            scores.append(score)
            print(f"Processed batch {i+1}/{num_batches}, score: {score}")
    
    return np.mean(scores) if scores else 0.0


def main():
    parser = argparse.ArgumentParser(description='Run distributed K-means clustering using Spark')
    parser.add_argument('-f', '--file', 
                       required=True,
                       help='Name of the CSV file in HDFS /data/ directory')
    
    args = parser.parse_args()
    datafile = args.file
    
    start_time = time.time()
    
    # Initialize Spark Session with reduced memory requirements
    spark = SparkSession \
        .builder \
        .appName("kmeans_spark_hdfs") \
        .master("yarn") \
        .config("spark.executor.instances", "2") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.memory", "512m") \
        .config("spark.executor.memoryOverhead", "128m") \
        .config("spark.driver.memory", "512m") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    end_time_system = time.time()
    
    config = {
        "datafile": datafile,
        "n_clusters": 16,
        "num_executors": 2,
        "cores_per_executor": 2,
        "batch_size_rows": 50000  # Process in batches of 50k rows
    }
    
    print(f"Starting K-means clustering with config: {config}")
    
    # Run distributed K-means
    res_score = distributed_kmeans_spark(spark, config)
    end_time = time.time()
    
    # Display results
    display_results(config, start_time, end_time, end_time_system, res_score)
    
    print(f"\n=== Timing Results ===")
    print(f"System initialization time: {end_time_system - start_time:.2f} seconds")
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
    print(f"Algorithm execution time: {end_time - end_time_system:.2f} seconds")
    print(f"Final Calinski-Harabasz Score: {res_score}")
    
    spark.stop()


if __name__ == "__main__":
    main()
