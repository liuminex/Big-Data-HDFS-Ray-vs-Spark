import argparse
import os
import resource
import sys
import time

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, when, lit
from pyspark.sql.types import DoubleType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def display_results(config, start_time, end_time, convergence_iterations, top_nodes):
    """Display and save PageRank results to console and file."""
    execution_time = end_time - start_time
    peak_memory_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    
    # Format top nodes output
    top_nodes_output = f"\nTop {len(top_nodes)} Nodes by PageRank Score:"
    for i, (node, score) in enumerate(top_nodes, 1):
        top_nodes_output += f"\n{i:3d}. {node}: {score:.6f}"
    
    # Display formatted results
    results_header = "PAGERANK ALGORITHM RESULTS (SPARK)"
    results_text = f"""
{'=' * 60}
{results_header:^60}
{'=' * 60}
Dataset: {config['datafile']}
Execution time: {execution_time:.2f} seconds
Peak memory usage: {peak_memory_mb:.2f} MB
Convergence iterations: {convergence_iterations}
Maximum iterations: {config['max_iterations']}

Algorithm Configuration:
• PageRank algorithm: Power iteration method
• Damping factor: {config['damping_factor']}
• Convergence threshold: {config['convergence_threshold']}
• Convergence sample size: {config.get('convergence_sample_size', 10000)}
• Graph representation: Spark DataFrames with joins
• Isolated node handling: Left join with null handling
{top_nodes_output}
{'=' * 60}
"""
    
    print(results_text)
    
    # Create results directory if it doesn't exist
    results_dir = 'results'
    os.makedirs(results_dir, exist_ok=True)
    
    # Generate standardized filename with timestamp
    timestamp = int(time.time())
    dataset_name = os.path.basename(config['datafile']).replace('.csv', '')
    filename = f'pagerank_spark_results_{dataset_name}_{timestamp}.txt'
    filepath = os.path.join(results_dir, filename)
    
    # Save results to file
    with open(filepath, 'w') as f:
        f.write(results_text)
    
    print(f"Results saved to {filepath}")
    print(f"{'=' * 60}")


def build_graph_spark(spark, config):
    """Build graph structure from CSV data with optimized Spark operations."""
    # Build graph structure from Reddit hyperlinks data with optimized partitioning
    hdfs_path = f"hdfs://o-master:54310/data/{config['datafile']}"
    
    # Read the CSV file from HDFS with optimized partitioning
    df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .option("multiline", "false") \
        .csv(hdfs_path) \
        .repartition(16)  # Ensure good distribution across executors
    
    # Cache the initial dataframe since we'll use it multiple times
    df.cache()
    
    # Select source and target subreddits with better memory management
    edges = df.select(
        col("SOURCE_SUBREDDIT").alias("source"),
        col("TARGET_SUBREDDIT").alias("target")
    ).filter(
        col("source").isNotNull() & col("target").isNotNull()
    ).distinct().persist()  # Persist edges as they're used multiple times
    
    # Get all unique nodes
    source_nodes = edges.select(col("source").alias("node"))
    target_nodes = edges.select(col("target").alias("node"))
    nodes = source_nodes.union(target_nodes).distinct().persist()
    
    # Calculate out-degree for each node
    out_degrees = edges.groupBy("source").agg(count("target").alias("out_degree")).persist()
    
    print(f"Graph statistics:")
    print(f"  Total nodes: {nodes.count()}")
    print(f"  Total edges: {edges.count()}")
    
    return nodes, edges, out_degrees


def pagerank_spark(spark, config):
    """Execute distributed PageRank algorithm using Spark."""
    # Distributed PageRank implementation using Spark
    print("Building graph structure...")
    nodes, edges, out_degrees = build_graph_spark(spark, config)
    
    # Initialize PageRank scores
    total_nodes = nodes.count()
    initial_score = 1.0 / total_nodes
    
    # Initialize current scores with proper partitioning
    current_scores = nodes.withColumn("score", lit(initial_score)).persist()
    
    damping_factor = config["damping_factor"]
    convergence_threshold = config["convergence_threshold"]
    max_iterations = config["max_iterations"]
    
    # Standardized convergence parameters
    convergence_sample_size = min(config.get("convergence_sample_size", 10000), total_nodes)
    convergence_sample_fraction = convergence_sample_size / total_nodes
    
    print(f"PageRank configuration:")
    print(f"  Total nodes: {total_nodes}")
    print(f"  Damping factor: {damping_factor}")
    print(f"  Convergence threshold: {convergence_threshold}")
    print(f"  Convergence sample size: {convergence_sample_size}")
    print(f"  Max iterations: {max_iterations}")
    
    print(f"Starting PageRank iterations...")
    
    for iteration in range(max_iterations):
        print(f"Iteration {iteration + 1}/{max_iterations}")
        
        # Join current scores with edges and out-degrees with broadcast optimization
        scores_with_edges = edges.join(current_scores, edges.source == current_scores.node, "inner") \
                                .join(out_degrees, edges.source == out_degrees.source, "inner") \
                                .select(
                                    col("target").alias("node"),
                                    (col("score") * damping_factor / col("out_degree")).alias("contribution")
                                ).repartition(16, "node")  # Repartition by node for better aggregation
        
        # Sum contributions for each node
        new_contributions = scores_with_edges.groupBy("node").agg(
            spark_sum("contribution").alias("total_contribution")
        ).persist()
        
        # Calculate new PageRank scores with proper isolated node handling
        new_scores = current_scores.join(new_contributions, "node", "left") \
                                  .withColumn("total_contribution", 
                                            when(col("total_contribution").isNull(), 0.0)
                                            .otherwise(col("total_contribution"))) \
                                  .withColumn("new_score", 
                                            (1.0 - damping_factor) / total_nodes + 
                                            col("total_contribution")) \
                                  .select("node", col("new_score").alias("score")) \
                                  .persist()
        
        # Standardized convergence check
        if iteration > 0:
            # Use deterministic sampling for convergence check
            sample_fraction = convergence_sample_fraction if convergence_sample_size < total_nodes else 1.0
            
            # Calculate L2 norm of differences with consistent sampling
            score_diff_row = current_scores.alias("old").join(new_scores.alias("new"), "node", "inner") \
                                 .sample(False, sample_fraction, seed=42) \
                                 .withColumn("diff", 
                                           (col("old.score") - col("new.score")) * 
                                           (col("old.score") - col("new.score"))) \
                                 .agg(spark_sum("diff").alias("total_diff")) \
                                 .first()
            
            score_diff = score_diff_row["total_diff"] if score_diff_row and score_diff_row["total_diff"] is not None else 0.0
            
            # Scale for sampling if needed
            if sample_fraction < 1.0:
                score_diff = score_diff / sample_fraction
            
            actual_sample_count = int(total_nodes * sample_fraction)
            print(f"  Convergence metric (L2 norm): {score_diff:.8f}")
            print(f"  Sample size: {actual_sample_count} / {total_nodes}")
            
            if score_diff < convergence_threshold:
                print(f"Converged after {iteration + 1} iterations")
                break
        
        # Unpersist old scores to free memory
        current_scores.unpersist()
        new_contributions.unpersist()
        current_scores = new_scores
    
    # Get top nodes by PageRank score
    top_nodes = current_scores.orderBy(col("score").desc()).limit(10).collect()
    top_nodes_list = [(row.node, row.score) for row in top_nodes]
    
    # Clean up persisted DataFrames
    current_scores.unpersist()
    edges.unpersist()
    nodes.unpersist()
    out_degrees.unpersist()
    
    return iteration + 1, top_nodes_list


def main():
    """Parse arguments and run PageRank benchmark with Spark."""
    parser = argparse.ArgumentParser(description='Distributed PageRank using Spark')
    parser.add_argument('-f', '--datafile', type=str, required=True,
                       help='Input CSV file name in HDFS /data/ directory')
    parser.add_argument('--damping-factor', type=float, default=0.85,
                       help='Damping factor for PageRank algorithm')
    parser.add_argument('--max-iterations', type=int, default=20,
                       help='Maximum number of iterations')
    parser.add_argument('--convergence-threshold', type=float, default=1e-6,
                       help='Convergence threshold for PageRank')
    parser.add_argument('--convergence-sample-size', type=int, default=10000,
                       help='Sample size for convergence checking on large graphs')
    
    args = parser.parse_args()
    
    config = {
        'datafile': args.datafile,
        'damping_factor': args.damping_factor,
        'max_iterations': args.max_iterations,
        'convergence_threshold': args.convergence_threshold,
        'convergence_sample_size': args.convergence_sample_size
    }
    
    start_time = time.time()
    
    # Initialize Spark Session with optimized configuration for 2VMs with 4GB RAM each
    spark = SparkSession.builder \
        .appName("PageRank_Distributed") \
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
        print("Starting distributed PageRank with Spark...")
        convergence_iterations, top_nodes = pagerank_spark(spark, config)
        
        end_time = time.time()
        display_results(config, start_time, end_time, convergence_iterations, top_nodes)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
