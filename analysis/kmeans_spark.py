import argparse
import os
import resource
import sys
import time

import numpy as np
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def display_results(config, start_time, end_time, centroids, sample_data):
    """Display and save K-Means clustering results to console and file."""
    execution_time = end_time - start_time
    peak_memory_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    
    # Format centroids output
    centroids_output = "\nFinal Cluster Centroids:"
    for i, centroid in enumerate(centroids):
        centroids_output += f"\nCluster {i}: [{', '.join(f'{x:.4f}' for x in centroid)}]"
    
    # Format sample data output
    sample_output = "\nSample Clustered Data:"
    for i, row in enumerate(sample_data[:5]):  # Limit to first 5 samples
        features_str = '[' + ', '.join(f'{x:.4f}' for x in row.features) + ']'
        sample_output += f"\n  Features: {features_str} -> Cluster: {row.cluster}"
    
    # Display formatted results
    results_header = "K-MEANS CLUSTERING RESULTS (SPARK)"
    results_text = f"""
{'=' * 60}
{results_header:^60}
{'=' * 60}
Dataset: {config['datafile']}
Execution time: {execution_time:.2f} seconds
Peak memory usage: {peak_memory_mb:.2f} MB
Number of clusters (K): {config['clusters']}
Maximum iterations: {config['max_iterations']}

Algorithm Configuration:
• Clustering method: K-Means with Lloyd's algorithm
• Convergence criterion: Centroid shift < {config.get('convergence_tolerance', 1e-4)}
• Initialization: k-means++
• Random seed: {config.get('random_seed', 42)}
• Features used: 8 text analysis features
{centroids_output}
{sample_output}
{'=' * 60}
"""
    
    print(results_text)
    
    # Create results directory if it doesn't exist
    results_dir = 'results'
    os.makedirs(results_dir, exist_ok=True)
    
    # Generate standardized filename with timestamp
    timestamp = int(time.time())
    dataset_name = os.path.basename(config['datafile']).replace('.csv', '')
    filename = f'kmeans_spark_results_{dataset_name}_{timestamp}.txt'
    filepath = os.path.join(results_dir, filename)
    
    # Save results to file
    with open(filepath, 'w') as f:
        f.write(results_text)
    
    print(f"Results saved to {filepath}")
    print(f"{'=' * 60}")


def load_and_prepare_data(spark, config):
    """Load data from HDFS and prepare feature vectors for clustering."""
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
    """Execute distributed K-Means clustering using Spark MLlib."""
    # Distributed K-Means implementation using Spark MLlib
    print("Preparing data for K-Means clustering...")
    df_features = load_and_prepare_data(spark, config)
    
    k = config["clusters"]
    max_iter = config["max_iterations"]
    convergence_tolerance = config.get('convergence_tolerance', 1e-4)
    random_seed = config.get('random_seed', 42)
    
    print(f"K-Means configuration:")
    print(f"  Clusters: {k}")
    print(f"  Max iterations: {max_iter}")
    print(f"  Convergence tolerance: {convergence_tolerance}")
    print(f"  Random seed: {random_seed}")
    print(f"  Initialization: k-means++")
    
    print(f"Starting K-Means clustering with K={k}, max_iter={max_iter}...")
    
    # Configure and train K-Means model with explicit parameters
    kmeans = KMeans() \
        .setK(k) \
        .setMaxIter(max_iter) \
        .setFeaturesCol("features") \
        .setPredictionCol("cluster") \
        .setSeed(random_seed) \
        .setInitMode("k-means||") \
        .setTol(convergence_tolerance)
    
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
    """Parse arguments and run K-Means benchmark with Spark."""
    parser = argparse.ArgumentParser(description='Distributed K-Means clustering using Spark')
    parser.add_argument('-f', '--datafile', type=str, required=True,
                       help='Input CSV file name in HDFS /data/ directory')
    parser.add_argument('-k', '--clusters', type=int, default=3,
                       help='Number of clusters for K-Means')
    parser.add_argument('--max-iterations', type=int, default=20,
                       help='Maximum number of iterations')
    parser.add_argument("--convergence-tolerance", type=float, default=1e-4,
                       help="Convergence tolerance for centroid shift")
    parser.add_argument("--random-seed", type=int, default=42,
                       help="Random seed for reproducibility")
    
    args = parser.parse_args()
    
    config = {
        "datafile": args.datafile,
        "clusters": args.clusters,
        "max_iterations": args.max_iterations,
        "convergence_tolerance": args.convergence_tolerance,
        "random_seed": args.random_seed
    }
    
    start_time = time.time()
    
    # Initialize Spark Session with minimal configuration for distributed setup
    spark = SparkSession.builder \
        .appName("KMeans_Distributed") \
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
