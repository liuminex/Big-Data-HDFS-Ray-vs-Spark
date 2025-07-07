from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from graphframes import GraphFrame
from sparkmeasure import StageMetrics
import sys
import time

# pip install graphframes
# pip install sparkmeasure


def main():
    # Parse command line for executor count or default
    if len(sys.argv) > 1:
        num_executors = sys.argv[1]
    else:
        num_executors = "4"

    spark = SparkSession.builder \
        .appName("triangle counting") \
        .master("yarn") \
        .config("spark.executor.instances", num_executors) \
        .config("spark.executor.memory", "512m") \
        .config("spark.executor.memoryOverhead", "128m") \
        .config("spark.jars.packages", "ch.cern.sparkmeasure:spark-measure_2.12:0.23,graphframes:graphframes:0.8.3-spark3.5-s_2.12") \
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
        .load("hdfs:///data/data_reddit_10M.csv")

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

    # Print triangle counts per chunk equivalent (optional)
    print("Sample triangle counts:")
    for row in triangle_counts[:10]:  # print first 10
        print(f"Node {row['id']} has {row['count']} triangles")

    # Or sum all triangles
    total_triangles = triangle_df.agg({"count": "sum"}).collect()[0][0]
    print(f"Total number of triangles in the graph: {total_triangles}")

    print("Total Time:", time.time() - start_time)

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
