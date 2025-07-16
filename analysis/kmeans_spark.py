import sys
import os
import time
import argparse
import numpy as np
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, count
import resource

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def display_results(config, start_time, end_time, centroids, sample_data):
    execution_time = end_time - start_time
    peak_memory = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # MB on Linux
    
    results_text = f"""
Dataset: {config['datafile']}
Total execution time: {execution_time:.2f} seconds
Peak memory usage: {peak_memory:.2f} MB
Number of clusters (K): {config['k_clusters']}
Maximum iterations: {config['max_iterations']}

Final Centroids:
"""
    
    for i, centroid in enumerate(centroids):
        results_text += f"Cluster {i}: {centroid}\n"
    
    results_text += f"\nSample clustered data:\n"
    for row in sample_data:
        results_text += f"Features: {row.features}, Cluster: {row.cluster}\n"
    
    print(results_text)
    
    timestamp = int(time.time())
    filename = f'kmeans_spark_results_{config["datafile"].replace(".csv", "")}_{timestamp}.txt'
    if not os.path.exists('results'):
        os.makedirs('results')
    with open(f'results/{filename}', 'w') as f:
        f.write(results_text)
    
    print(f"Results saved to results/{filename}")


def load_and_prepare_data(spark, config):
    # Load data from HDFS with optimized partitioning
    hdfs_path = f"hdfs://o-master:54310/data/{config['datafile']}"
    
    print("Loading data from HDFS...")
    df = spark.read.format("csv") \
           .option("header", "true") \
           .option("inferSchema", "true") \
           .option("multiline", "false") \
           .csv(hdfs_path) \
           .repartition(16)
    
    # Cache the initial dataframe since we'll use it multiple times
    df.cache()
    
    print(f"Dataset statistics:")
    print(f"  Total rows: {df.count()}")
    print(f"  Total columns: {len(df.columns)}")
    
    # Select features for clustering
    selected_features = ['FracSpecialChars', 'NumWords',
                        'AvgCharsPerSentence', 'AvgWordsPerSentence', 'AutomatedReadabilityIndex',
                        'SentimentPositive', 'SentimentNegative', 'SentimentCompound']
    
    print(f"  Selected features: {selected_features}")
    
    # Assemble features into vector format
    assembler = VectorAssembler(
        inputCols=selected_features,
        outputCol="features",
        handleInvalid="skip"
    )
    
    df_features = assembler.transform(df).select("features").persist()
    
    # Show statistics of prepared data
    features_count = df_features.count()
    print(f"  Rows with valid features: {features_count}")
    
    return df_features


def kmeans_spark(spark, config):
    # Distributed K-Means implementation using Spark MLlib
    print("Preparing data for K-Means clustering...")
    df_features = load_and_prepare_data(spark, config)
    
    K = config["k_clusters"]
    MAX_ITER = config["max_iterations"]
    
    print(f"Starting K-Means clustering with K={K}, max_iter={MAX_ITER}...")
    
    # Configure and train K-Means model
    kmeans = KMeans() \
        .setK(K) \
        .setMaxIter(MAX_ITER) \
        .setFeaturesCol("features") \
        .setPredictionCol("cluster") \
        .setSeed(42)
    
    # Fit the model
    model = kmeans.fit(df_features)
    
    # Get cluster centroids
    centroids = model.clusterCenters()
    
    # Transform data to get cluster assignments
    df_clustered = model.transform(df_features)
    
    # Get sample of clustered data for display
    sample_data = df_clustered.limit(5).collect()
    
    # Show cluster distribution
    cluster_counts = df_clustered.groupBy("cluster").count().orderBy("cluster").collect()
    print(f"\nCluster distribution:")
    for row in cluster_counts:
        print(f"  Cluster {row.cluster}: {row.count} points")
    
    # Clean up persisted DataFrames
    df_features.unpersist()
    
    return centroids, sample_data


def main():
    parser = argparse.ArgumentParser(description='Run distributed K-Means clustering using Spark')
    parser.add_argument('-f', '--file', 
                       required=True,
                       help='Name of the CSV file in HDFS /data/ directory')
    parser.add_argument('-k', '--clusters', type=int, default=3,
                       help='Number of clusters (default: 3)')
    parser.add_argument('--max-iterations', type=int, default=20,
                       help='Maximum number of iterations (default: 20)')
    
    args = parser.parse_args()
    
    config = {
        "datafile": args.file,
        "k_clusters": args.clusters,
        "max_iterations": args.max_iterations
    }
    
    start_time = time.time()
    
    # Initialize Spark Session with optimized configuration for 2VMs with 4GB RAM each
    spark = SparkSession.builder \
        .appName("KMeans_Distributed") \
        .master("yarn") \
        .config("spark.executor.instances", "4") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.memory", "1200m") \
        .config("spark.executor.memoryOverhead", "300m") \
        .config("spark.driver.memory", "800m") \
        .config("spark.driver.maxResultSize", "400m") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrationRequired", "false") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "16") \
        .config("spark.default.parallelism", "16") \
        .config("spark.sql.files.maxPartitionBytes", "64MB") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .getOrCreate()
    
    try:
        print("Starting distributed K-Means clustering with Spark...")
        centroids, sample_data = kmeans_spark(spark, config)
        
        end_time = time.time()
        display_results(config, start_time, end_time, centroids, sample_data)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
