import sys
import os
import time
import argparse
import resource
import networkx as nx
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def display_results(config, start_time, end_time, results, total_triangles):
    execution_time = end_time - start_time
    peak_memory = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    results_text = f"""
Dataset: {config['file']}
Total execution time: {execution_time:.2f} seconds
Peak memory usage: {peak_memory:.2f} MB
Total triangles found: {total_triangles}
"""
    print(results_text)
    timestamp = int(time.time())
    filename = f'triangles_spark_results_{os.path.basename(config["file"]).replace(".csv", "")}_{timestamp}.txt'
    if not os.path.exists('results'):
        os.makedirs('results')
    with open(f'results/{filename}', 'w') as f:
        f.write(results_text)
    print(f"Results saved to results/{filename}")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Count triangles in a graph using Spark (NetworkX on driver)')
    parser.add_argument('-f', '--file', type=str, default='data_reddit_100M.csv',
                        help='Path to the input CSV file')
    args = parser.parse_args()
    config = {'file': args.file}

    # Initialize Spark session
    spark = SparkSession.builder.appName("TriangleCountingSpark").getOrCreate()
    start_time = time.time()
    hdfs_path = f"hdfs://o-master:54310/data/{args.file}"
    df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .option("multiline", "false") \
        .csv(hdfs_path)
    
    # Only keep source and target columns, drop nulls
    edges = df.select(
        col("SOURCE_SUBREDDIT").alias("src"),
        col("TARGET_SUBREDDIT").alias("dst")
    ).filter(col("src").isNotNull() & col("dst").isNotNull())    # Use distributed triangle counting algorithm
    
    # Step 1: Create adjacency lists distributed across cluster
    adjacency = edges.groupBy("src").agg(
        collect_list("dst").alias("neighbors")
    ).rdd.map(lambda row: (row.src, set(row.neighbors)))
    
    # Step 2: Broadcast adjacency for efficient lookups
    adjacency_broadcast = spark.sparkContext.broadcast(dict(adjacency.collect()))
    
    # Step 3: Distribute triangle counting across edges
    def count_triangles_for_edge(edge):
        src, dst = edge
        adj_dict = adjacency_broadcast.value
        
        # Get neighbors of both nodes
        src_neighbors = adj_dict.get(src, set())
        dst_neighbors = adj_dict.get(dst, set())
        
        # Find common neighbors (triangles)
        common_neighbors = src_neighbors.intersection(dst_neighbors)
        return len(common_neighbors)
    
    # Step 4: Count triangles in parallel
    edge_rdd = edges.rdd.map(lambda row: (str(row.src), str(row.dst)))
    triangle_counts = edge_rdd.map(count_triangles_for_edge)
    total_triangles = triangle_counts.sum()
    
    # Create results in expected format
    results = [{"distributed_count": total_triangles}]
    end_time = time.time()
    display_results(config, start_time, end_time, results, total_triangles)
    spark.stop()

if __name__ == "__main__":
    main()
