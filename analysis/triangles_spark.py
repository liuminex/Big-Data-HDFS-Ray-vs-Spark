from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from graphframes import GraphFrame
from sparkmeasure import StageMetrics
import sys
import time
import argparse
import os

def display_results(config, start_time, end_time, total_triangles, triangle_counts_sample):
    execution_time = end_time - start_time
    
    results_text = f"""
Dataset: {config['file']}
Total execution time: {execution_time:.2f} seconds
Number of executors: {config['num_executors']}
Total triangles found: {total_triangles}

Sample triangle counts per node:
"""
    
    for row in triangle_counts_sample:
        results_text += f"Node {row['id']}: {row['count']} triangles\n"
    
    print(results_text)
    
    timestamp = int(time.time())
    filename = f'counting_triangles_spark_results_{os.path.basename(config["file"]).replace(".csv", "")}_{timestamp}.txt'
    if not os.path.exists('results'):
        os.makedirs('results')
    with open(f'results/{filename}', 'w') as f:
        f.write(results_text)
    
    print(f"Results saved to results/{filename}")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run triangle counting using Spark')
    parser.add_argument('-f', '--file', 
                       required=True,
                       help='Name of the CSV file in HDFS /data/ directory')
    parser.add_argument('--num-executors', type=str, default="4",
                       help='Number of Spark executors (default: 4)')
    
    args = parser.parse_args()
    datafile = args.file
    num_executors = args.num_executors    # Optimized Spark configuration for 2VMs with 4GB RAM each - FORCE proper distribution like Ray
    spark = SparkSession.builder \
        .appName("TriangleCounting_Distributed") \
        .master("yarn") \
        .config("spark.executor.instances", "4") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.memory", "1200m") \
        .config("spark.executor.memoryOverhead", "300m") \
        .config("spark.driver.memory", "800m") \
        .config("spark.driver.maxResultSize", "400m") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "32MB") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.shuffle.partitions", "32") \
        .config("spark.default.parallelism", "32") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.locality.wait", "0s") \
        .config("spark.locality.wait.node", "0s") \
        .config("spark.locality.wait.rack", "0s") \
        .config("spark.locality.wait.process", "0s") \
        .config("spark.scheduler.maxRegisteredResourcesWaitingTime", "30s") \
        .config("spark.scheduler.minRegisteredResourcesRatio", "1.0") \
        .config("spark.task.maxAttempts", "1") \
        .config("spark.stage.maxConsecutiveAttempts", "1") \
        .config("spark.scheduler.mode", "FAIR") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.checkpoint.compress", "true") \
        .config("spark.speculation", "false") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "false") \
        .config("spark.jars.packages", "ch.cern.sparkmeasure:spark-measure_2.12:0.23,graphframes:graphframes:0.8.4-spark3.5-s_2.12") \
        .getOrCreate()

    sc = spark.sparkContext
    stagemetrics = StageMetrics(spark)
    stagemetrics.begin()

    # Define schema for CSV file (adjust delimiter if needed)
    schema = StructType([
        StructField("src", StringType(), True),
        StructField("dst", StringType(), True)
    ])    # Load edges from HDFS CSV with aggressive partitioning for large datasets
    edges_df = spark.read.format("csv") \
        .option("header", "false") \
        .option("comment", "#") \
        .option("delimiter", ",") \
        .schema(schema) \
        .load(f"hdfs:///data/{datafile}") \
        .repartition(32)  # Increase partitions for better distribution
    
    # Cache edges as they'll be used multiple times
    edges_df.cache()

    # Create vertices DataFrame with aggressive partitioning
    vertices_df = edges_df.select("src").union(edges_df.select("dst")).distinct() \
        .withColumnRenamed("src", "id") \
        .repartition(16)  # More partitions for vertices
    
    # Cache vertices
    vertices_df.cache()
    
    # Build the graph
    graph = GraphFrame(vertices_df, edges_df)
    
    # Force evaluation to ensure caching is effective and check data distribution
    vertex_count = vertices_df.count()
    edge_count = edges_df.count()
    print(f"Graph loaded - Vertices: {vertex_count}, Edges: {edge_count}")
    print(f"Vertices partitions: {vertices_df.rdd.getNumPartitions()}")
    print(f"Edges partitions: {edges_df.rdd.getNumPartitions()}")

    start_time = time.time()
    
    # Enable checkpointing for large datasets to avoid recomputation
    spark.sparkContext.setCheckpointDir("hdfs:///tmp/spark-checkpoint")
    
    # Compute triangles with memory-optimized approach
    triangle_df = graph.triangleCount()
    
    # Checkpoint the result to avoid recomputation and enable spilling
    triangle_df.checkpoint()
    triangle_df.cache()
    
    # Force evaluation of triangle computation
    triangle_df.count()  # Trigger computation
    
    # Collect results in small batches to avoid driver memory issues
    triangle_counts = triangle_df.select("id", "count").limit(10).collect()

    # Use streaming aggregation for total count to avoid collecting all data
    total_triangles = triangle_df.agg({"count": "sum"}).collect()[0][0]
    
    end_time = time.time()

    # Create config dictionary for display_results
    config = {
        'file': datafile,
        'num_executors': num_executors
    }
    
    # Display and save results
    display_results(config, start_time, end_time, total_triangles, triangle_counts[:10])

    stagemetrics.end()
    stagemetrics.print_report()
    print(stagemetrics.aggregate_stagemetrics())

    # Memory report (may need retries)
    patience = 20
    while patience > 0:
        try:
            stagemetrics.print_memory_report()
            break
        except Exception:
            print("Memory report not ready yet, waiting...")
            time.sleep(1)
            patience -= 1
    else:
        print("Memory report never ready :(")

    sc.stop()


if __name__ == "__main__":
    main()
