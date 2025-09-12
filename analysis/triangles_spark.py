import argparse
import os
import resource
import sys
import time

import networkx as nx
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def display_results(config, start_time, end_time, results, total_triangles):
    """Display and save triangle counting results to console and file."""
    execution_time = end_time - start_time
    peak_memory_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    
    # Display formatted results
    results_header = "TRIANGLE COUNTING RESULTS (SPARK)"
    results_text = f"""
{'=' * 60}
{results_header:^60}
{'=' * 60}
Dataset: {config['datafile']}
Execution time: {execution_time:.2f} seconds
Peak memory usage: {peak_memory_mb:.2f} MB
Total triangles found: {total_triangles:,}

Algorithm Configuration:
• Counting method: Distributed edge-based intersection
• Graph representation: Spark DataFrames with broadcasts
• Processing strategy: Adjacency list broadcast + parallel computation
{'=' * 60}
"""
    
    print(results_text)
    
    # Create results directory if it doesn't exist
    results_dir = 'results'
    os.makedirs(results_dir, exist_ok=True)
    
    # Generate standardized filename with timestamp
    timestamp = int(time.time())
    dataset_name = os.path.basename(config['datafile']).replace('.csv', '')
    filename = f'triangles_spark_results_{dataset_name}_{timestamp}.txt'
    filepath = os.path.join(results_dir, filename)
    
    # Save results to file
    with open(filepath, 'w') as f:
        f.write(results_text)
    
    print(f"Results saved to {filepath}")
    print(f"{'=' * 60}")

def main():
    """Parse arguments and run triangle counting benchmark with Spark."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Triangle counting using distributed Spark')
    parser.add_argument('-f', '--datafile', type=str, required=True,
                       help='Input CSV file name in HDFS /data/ directory')
    args = parser.parse_args()
    config = {
        'datafile': args.datafile
    }

    # Initialize Spark session
    spark = SparkSession.builder.appName("TriangleCountingSpark").getOrCreate()
    start_time = time.time()
    hdfs_path = f"hdfs://o-master:54310/data/{config['datafile']}"
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
