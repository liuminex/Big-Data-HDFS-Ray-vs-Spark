import time
import ray
import subprocess
import tempfile
import os
import numpy as np
import pandas as pd
import argparse
import resource
from collections import defaultdict


def display_results(config, start_time, end_time, convergence_iterations, top_nodes):
    execution_time = end_time - start_time
    peak_memory = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    
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


@ray.remote
def build_graph_chunk(data_chunk):
    # Build graph structure from a chunk of data
    node_ip = ray._private.services.get_node_ip_address()
    print(f"(build_graph_chunk) Processing chunk on node: {node_ip}")
    
    # Extract edges from the chunk
    edges = []
    nodes = set()
    out_degrees = defaultdict(int)
    
    for _, row in data_chunk.iterrows():
        if pd.notna(row['SOURCE_SUBREDDIT']) and pd.notna(row['TARGET_SUBREDDIT']):
            source = str(row['SOURCE_SUBREDDIT'])
            target = str(row['TARGET_SUBREDDIT'])
            
            edges.append((source, target))
            nodes.add(source)
            nodes.add(target)
            out_degrees[source] += 1
    
    return edges, nodes, dict(out_degrees)


@ray.remote
def pagerank_update_chunk(edges_chunk, current_scores, out_degrees, damping_factor, total_nodes):
    # Update PageRank scores for a chunk of edges
    node_ip = ray._private.services.get_node_ip_address()
    print(f"(pagerank_update_chunk) Processing on node: {node_ip}")
    
    contributions = defaultdict(float)
    
    for source, target in edges_chunk:
        if source in current_scores and source in out_degrees:
            contribution = current_scores[source] / out_degrees[source]
            contributions[target] += contribution
    
    # Calculate new scores for nodes in this chunk
    new_scores = {}
    for node in set(target for _, target in edges_chunk):
        new_scores[node] = (1.0 - damping_factor) / total_nodes + damping_factor * contributions[node]
    
    return new_scores


def build_graph_ray(config):
    # Build graph structure using Ray from local dataset
   
    data_path = f"../data/{config['datafile']}"
    
    if not os.path.exists(data_path):
        raise Exception(f"Data file not found: {data_path}")
    
    print(f"Reading data from {data_path}...")
    
    chunk_rows = 50000
    
    # Read and process the file in chunks
    chunks = []
    for chunk_df in pd.read_csv(data_path, chunksize=chunk_rows):
        if len(chunk_df) > 0:
            chunks.append(chunk_df)
    
    print(f"Processing {len(chunks)} data chunks of {chunk_rows} rows each...")
    
    batch_size = 2
    all_edges = []
    all_nodes = set()
    combined_out_degrees = defaultdict(int)
    
    for i in range(0, len(chunks), batch_size):
        batch_chunks = chunks[i:i + batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(chunks) + batch_size - 1)//batch_size} with {len(batch_chunks)} chunks...")
        
        # Process current batch
        futures = [build_graph_chunk.remote(chunk) for chunk in batch_chunks]
        results = ray.get(futures)
        
        # Combine results
        for edges, nodes, out_degrees in results:
            all_edges.extend(edges)
            all_nodes.update(nodes)
            for node, degree in out_degrees.items():
                combined_out_degrees[node] += degree
        
        print(f"  Progress: {len(all_nodes)} nodes, {len(all_edges)} edges so far")
    
    # Remove duplicate edges
    unique_edges = list(set(all_edges))
    
    print(f"Graph statistics:")
    print(f"  Total nodes: {len(all_nodes)}")
    print(f"  Total edges: {len(unique_edges)}")
    
    return unique_edges, all_nodes, dict(combined_out_degrees)


def pagerank_ray(config):
    # Distributed PageRank implementation using Ray
    print("Building graph structure...")
    edges, nodes, out_degrees = build_graph_ray(config)
    
    # Initialize PageRank scores
    total_nodes = len(nodes)
    initial_score = 1.0 / total_nodes
    current_scores = {node: initial_score for node in nodes}
    
    damping_factor = config["damping_factor"]
    convergence_threshold = config["convergence_threshold"]
    max_iterations = config["max_iterations"]
    
    # Partition edges for parallel processing
    num_partitions = min(len(edges) // 1000 + 1, int(ray.cluster_resources().get('CPU', 1)))
    edges_per_partition = max(1, len(edges) // num_partitions)
    edge_chunks = [edges[i:i + edges_per_partition] 
                   for i in range(0, len(edges), edges_per_partition)]
    
    print(f"Starting PageRank iterations with {num_partitions} partitions...")
    
    for iteration in range(max_iterations):
        print(f"Iteration {iteration + 1}/{max_iterations}")
        
        # Distribute PageRank computation across chunks
        futures = [
            pagerank_update_chunk.remote(
                chunk, current_scores, out_degrees, damping_factor, total_nodes
            ) for chunk in edge_chunks
        ]
        
        # Collect results
        chunk_results = ray.get(futures)
        
        # Merge results
        new_scores = {node: (1.0 - damping_factor) / total_nodes for node in nodes}
        
        for chunk_scores in chunk_results:
            for node, score in chunk_scores.items():
                new_scores[node] = score
        
        # Check for convergence
        if iteration > 0:
            diff = sum((current_scores[node] - new_scores[node]) ** 2 
                      for node in nodes)
            
            print(f"  Convergence metric: {diff:.8f}")
            
            if diff < convergence_threshold:
                print(f"Converged after {iteration + 1} iterations")
                break
        
        current_scores = new_scores
    
    # Get top nodes by PageRank score
    sorted_nodes = sorted(current_scores.items(), key=lambda x: x[1], reverse=True)
    top_nodes = sorted_nodes[:10]
    
    return iteration + 1, top_nodes


def main():
    parser = argparse.ArgumentParser(description='Run distributed PageRank using Ray')
    parser.add_argument('-f', '--file', 
                       required=True,
                       help='Name of the CSV file in HDFS /data/ directory')
    parser.add_argument('--damping-factor', type=float, default=0.85,
                       help='PageRank damping factor (default: 0.85)')
    parser.add_argument('--max-iterations', type=int, default=20,
                       help='Maximum number of iterations (default: 20)')
    parser.add_argument('--convergence-threshold', type=float, default=1e-6,
                       help='Convergence threshold (default: 1e-6)')
    parser.add_argument('--batch-size', type=int, default=64*1024*1024,
                       help='Batch size for reading data (default: 64MB)')
    
    args = parser.parse_args()
    
    config = {
        "datafile": args.file,
        "damping_factor": args.damping_factor,
        "max_iterations": args.max_iterations,
        "convergence_threshold": args.convergence_threshold,
        "batch_size": args.batch_size
    }
    
    start_time = time.time()
    
    # Initialize Ray
    if not ray.is_initialized():
        ray.init(address="auto")
    
    try:
        print("Starting distributed PageRank with Ray...")
        convergence_iterations, top_nodes = pagerank_ray(config)
        
        end_time = time.time()
        display_results(config, start_time, end_time, convergence_iterations, top_nodes)
        
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
