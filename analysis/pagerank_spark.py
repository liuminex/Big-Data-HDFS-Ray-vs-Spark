import sys
import os
import time
import argparse
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, when, lit
from pyspark.sql.types import DoubleType
import resource

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def display_results(config, start_time, end_time, convergence_iterations, top_nodes):
    execution_time = end_time - start_time
    peak_memory = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # MB on Linux
    
    results_text = f"""
Dataset: {config['datafile']}
Total execution time: {execution_time:.2f} seconds
Peak memory usage: {peak_memory:.2f} MB
Convergence iterations: {convergence_iterations}
Damping factor: {config['damping_factor']}
Convergence threshold: {config['convergence_threshold']}

Top {len(top_nodes)} nodes by PageRank score:
"""
    
    for i, (node, score) in enumerate(top_nodes, 1):
        results_text += f"{i}. {node}: {score:.6f}\n"
    
    print(results_text)
    
    timestamp = int(time.time())
    filename = f'pagerank_spark_results_{config['datafile'].replace('.csv', '')}_{timestamp}.txt'
    if not os.path.exists('results'):
        os.makedirs('results')
    with open(f'results/{filename}', 'w') as f:
        f.write(results_text)
    
    print(f"Results saved to results/{filename}")


def build_graph_spark(spark, config):
    # Build graph structure from Reddit hyperlinks data
    hdfs_path = f"hdfs://o-master:54310/data/{config['datafile']}"
    
    # Read the CSV file from HDFS
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(hdfs_path)
    
    # Select source and target subreddits
    edges = df.select(
        col("SOURCE_SUBREDDIT").alias("source"),
        col("TARGET_SUBREDDIT").alias("target")
    ).filter(
        col("source").isNotNull() & col("target").isNotNull()
    ).distinct()
    
    # Get all unique nodes
    source_nodes = edges.select(col("source").alias("node"))
    target_nodes = edges.select(col("target").alias("node"))
    nodes = source_nodes.union(target_nodes).distinct()
    
    # Calculate out-degree for each node
    out_degrees = edges.groupBy("source").agg(count("target").alias("out_degree"))
    
    print(f"Graph statistics:")
    print(f"  Total nodes: {nodes.count()}")
    print(f"  Total edges: {edges.count()}")
    
    return nodes, edges, out_degrees


def pagerank_spark(spark, config):
    # Distributed PageRank implementation using Spark
    print("Building graph structure...")
    nodes, edges, out_degrees = build_graph_spark(spark, config)
    
    # Initialize PageRank scores
    total_nodes = nodes.count()
    initial_score = 1.0 / total_nodes
    
    # Initialize current scores
    current_scores = nodes.withColumn("score", lit(initial_score))
    
    damping_factor = config["damping_factor"]
    convergence_threshold = config["convergence_threshold"]
    max_iterations = config["max_iterations"]
    
    print(f"Starting PageRank iterations...")
    
    for iteration in range(max_iterations):
        print(f"Iteration {iteration + 1}/{max_iterations}")
        
        # Join current scores with edges and out-degrees
        scores_with_edges = edges.join(current_scores, edges.source == current_scores.node, "inner") \
                                .join(out_degrees, edges.source == out_degrees.source, "inner") \
                                .select(
                                    col("target").alias("node"),
                                    (col("score") / col("out_degree")).alias("contribution")
                                )
        
        # Sum contributions for each node
        new_contributions = scores_with_edges.groupBy("node").agg(
            spark_sum("contribution").alias("total_contribution")
        )
        
        # Calculate new PageRank scores
        new_scores = current_scores.join(new_contributions, "node", "left") \
                                  .withColumn("total_contribution", 
                                            when(col("total_contribution").isNull(), 0.0)
                                            .otherwise(col("total_contribution"))) \
                                  .withColumn("new_score", 
                                            (1.0 - damping_factor) / total_nodes + 
                                            damping_factor * col("total_contribution")) \
                                  .select("node", col("new_score").alias("score"))
        
        # Check for convergence
        if iteration > 0:
            # Calculate the difference between old and new scores
            score_diff = current_scores.alias("old").join(new_scores.alias("new"), "node", "inner") \
                                     .withColumn("diff", 
                                               (col("old.score") - col("new.score")) * 
                                               (col("old.score") - col("new.score"))) \
                                     .agg(spark_sum("diff").alias("total_diff")) \
                                     .collect()[0]["total_diff"]
            
            print(f"  Convergence metric: {score_diff:.8f}")
            
            if score_diff < convergence_threshold:
                print(f"Converged after {iteration + 1} iterations")
                break
        
        current_scores = new_scores
    
    # Get top nodes by PageRank score
    top_nodes = current_scores.orderBy(col("score").desc()).limit(10).collect()
    top_nodes_list = [(row.node, row.score) for row in top_nodes]
    
    return iteration + 1, top_nodes_list


def main():
    parser = argparse.ArgumentParser(description='Run distributed PageRank using Spark')
    parser.add_argument('-f', '--file', 
                       required=True,
                       help='Name of the CSV file in HDFS /data/ directory')
    parser.add_argument('--damping-factor', type=float, default=0.85,
                       help='PageRank damping factor (default: 0.85)')
    parser.add_argument('--max-iterations', type=int, default=20,
                       help='Maximum number of iterations (default: 20)')
    parser.add_argument('--convergence-threshold', type=float, default=1e-6,
                       help='Convergence threshold (default: 1e-6)')
    
    args = parser.parse_args()
    
    config = {
        "datafile": args.file,
        "damping_factor": args.damping_factor,
        "max_iterations": args.max_iterations,
        "convergence_threshold": args.convergence_threshold
    }
    
    start_time = time.time()
    
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("PageRank") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
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
