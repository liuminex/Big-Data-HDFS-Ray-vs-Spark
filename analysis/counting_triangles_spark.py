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
    num_executors = args.num_executors

    # Optimized Spark configuration for 2GB YARN memory limit
    spark = SparkSession.builder \
        .appName("triangle counting") \
        .master("yarn") \
        .config("spark.executor.instances", num_executors) \
        .config("spark.executor.memory", "512m") \
        .config("spark.executor.memoryOverhead", "128m") \
        .config("spark.jars.packages", "ch.cern.sparkmeasure:spark-measure_2.12:0.23,graphframes:graphframes:0.8.4-spark3.5-s_2.12") \
        .getOrCreate()

    sc = spark.sparkContext
    stagemetrics = StageMetrics(spark)
    stagemetrics.begin()

    # Define schema for CSV file (adjust delimiter if needed)
    schema = StructType([
        StructField("src", StringType(), True),
        StructField("dst", StringType(), True)
    ])

    # Load edges from HDFS CSV (assumed comma separated)
    edges_df = spark.read.format("csv") \
        .option("header", "false") \
        .option("comment", "#") \
        .option("delimiter", ",") \
        .schema(schema) \
        .load(f"hdfs:///data/{datafile}")

    # Create vertices DataFrame (unique nodes)
    vertices_df = edges_df.select("src").union(edges_df.select("dst")).distinct() \
        .withColumnRenamed("src", "id")
    
    # Build the graph
    graph = GraphFrame(vertices_df, edges_df)

    start_time = time.time()
    # Compute triangles for each vertex
    triangle_df = graph.triangleCount()
    # Collect results (if you want per-node counts)
    triangle_counts = triangle_df.select("id", "count").collect()

    # Sum all triangles
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
