import sys
import os
import time
import argparse
import resource
import networkx as nx
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def display_results(config, start_time, end_time, results, total_triangles):
    execution_time = end_time - start_time
    peak_memory = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    results_text = f"""
Dataset: {config['file']}
Total execution time: {execution_time:.2f} seconds
Peak memory usage: {peak_memory:.2f} MB
Number of chunks: {config['chunks']}
Total triangles found: {total_triangles}

Triangle counts per chunk:
"""
    for idx, result in enumerate(results):
        chunk_total = sum(result.values()) if isinstance(result, dict) else result
        results_text += f"Chunk {idx}: {chunk_total} triangles\n"
    print(results_text)
    timestamp = int(time.time())
    filename = f'counting_triangles_spark_results_{os.path.basename(config["file"]).replace(".csv", "")}_{timestamp}.txt'
    if not os.path.exists('results'):
        os.makedirs('results')
    with open(f'results/{filename}', 'w') as f:
        f.write(results_text)
    print(f"Results saved to results/{filename}")

def main():
    parser = argparse.ArgumentParser(description='Count triangles in a graph using Spark (NetworkX on driver)')
    parser.add_argument('-f', '--file', type=str, default='data_reddit_100M.csv',
                        help='Path to the input CSV file')
    parser.add_argument('-c', '--chunks', type=int, default=4,
                        help='Number of chunks to split the nodes into')
    args = parser.parse_args()
    config = {'file': args.file, 'chunks': args.chunks}
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
    ).filter(col("src").isNotNull() & col("dst").isNotNull())
    # Collect edge list to driver
    edge_list = edges.rdd.map(lambda row: (str(row.src), str(row.dst))).collect()
    # Build undirected NetworkX graph
    G = nx.parse_edgelist([f"{u} {v}" for u, v in edge_list], nodetype=str, create_using=nx.DiGraph())
    G = G.to_undirected()
    node_list = list(G.nodes())
    num_chunks = min(args.chunks, 8)
    min_chunk_size = max(100, len(node_list) // 16)
    node_chunk_size = max(min_chunk_size, len(node_list) // num_chunks)
    node_chunks = [
        node_list[node_chunk_size * i : node_chunk_size * (i + 1)] if i < num_chunks - 1
        else node_list[node_chunk_size * i :]
        for i in range(num_chunks)
    ]
    node_chunks = [chunk for chunk in node_chunks if len(chunk) > 0]
    print(f"Processing {len(node_chunks)} chunks with average size: {len(node_list) // len(node_chunks) if node_chunks else 0}")
    # Count triangles per chunk
    results = []
    for chunk in node_chunks:
        batch_size = min(500, len(chunk))
        total_triangles = {}
        for i in range(0, len(chunk), batch_size):
            batch_nodes = chunk[i:i+batch_size]
            batch_triangles = nx.triangles(G, nodes=batch_nodes)
            total_triangles.update(batch_triangles)
        results.append(total_triangles)
    end_time = time.time()
    total_triangles = sum([sum(result.values()) if isinstance(result, dict) else result for result in results])
    display_results(config, start_time, end_time, results, total_triangles)
    spark.stop()

if __name__ == "__main__":
    main()
