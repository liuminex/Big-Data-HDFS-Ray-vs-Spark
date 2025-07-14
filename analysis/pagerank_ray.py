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


@ray.remote(num_cpus=1, memory=400*1024*1024, scheduling_strategy="SPREAD")
def build_graph_chunk(data_chunk):
    # Build graph structure from a chunk of data
    node_ip = ray._private.services.get_node_ip_address()
    print(f"(build_graph_chunk) Processing chunk on node: {node_ip}")
    
    # Extract edges from the chunk with memory-efficient processing
    edges = []
    nodes = set()
    out_degrees = defaultdict(int)
    
    # Process in reasonable batches
    batch_size = 2000
    for i in range(0, len(data_chunk), batch_size):
        batch = data_chunk.iloc[i:i+batch_size]
        for _, row in batch.iterrows():
            if pd.notna(row['SOURCE_SUBREDDIT']) and pd.notna(row['TARGET_SUBREDDIT']):
                source = str(row['SOURCE_SUBREDDIT'])
                target = str(row['TARGET_SUBREDDIT'])
                
                edges.append((source, target))
                nodes.add(source)
                nodes.add(target)
                out_degrees[source] += 1
    
    return edges, nodes, dict(out_degrees)


@ray.remote(num_cpus=1, memory=300*1024*1024, scheduling_strategy="SPREAD")
def pagerank_update_chunk(edges_chunk, current_scores, out_degrees, damping_factor, total_nodes):
    # Update PageRank scores for a chunk of edges
    node_ip = ray._private.services.get_node_ip_address()
    print(f"(pagerank_update_chunk) Processing {len(edges_chunk)} edges on node: {node_ip}")
    
    contributions = defaultdict(float)
    
    # Process edges in reasonable batches
    batch_size = 1000
    for i in range(0, len(edges_chunk), batch_size):
        batch = edges_chunk[i:i+batch_size]
        for source, target in batch:
            if source in current_scores and source in out_degrees:
                contribution = current_scores[source] / out_degrees[source]
                contributions[target] += contribution
    
    # Calculate new scores for nodes in this chunk
    new_scores = {}
    unique_targets = set(target for _, target in edges_chunk)
    for node in unique_targets:
        new_scores[node] = (1.0 - damping_factor) / total_nodes + damping_factor * contributions[node]
    
    return new_scores


def build_graph_ray(config):
    # Build graph structure using Ray from local dataset
    data_path = f"../data/{config['datafile']}"
    
    if not os.path.exists(data_path):
        raise Exception(f"Data file not found: {data_path}")
    
    print(f"Reading data from {data_path}...")
    
    chunk_rows = 20000
    
    all_edges = []
    all_nodes = set()
    combined_out_degrees = defaultdict(int)
    
    chunk_count = 0
    batch_size = 2  # Process 2 chunks at a time maximum
    
    for chunk_df in pd.read_csv(data_path, chunksize=chunk_rows):
        if len(chunk_df) == 0:
            continue
            
        chunk_count += 1
        print(f"Processing chunk {chunk_count} ({len(chunk_df)} rows)...")
        
        # Process single chunk
        future = build_graph_chunk.remote(chunk_df)
        edges, nodes, out_degrees = ray.get(future)
        
        # Combine results immediately and clean up
        all_edges.extend(edges)
        all_nodes.update(nodes)
        for node, degree in out_degrees.items():
            combined_out_degrees[node] += degree
        
        # Clear variables to free memory
        del edges, nodes, out_degrees, chunk_df, future
        
        # Progress update every 10 chunks
        if chunk_count % 10 == 0:
            print(f"  Progress: {len(all_nodes)} nodes, {len(all_edges)} edges so far")
            
        # Force garbage collection every 20 chunks
        if chunk_count % 20 == 0:
            import gc
            gc.collect()
    
    # Remove duplicate edges efficiently
    print("Removing duplicate edges...")
    unique_edges = list(set(all_edges))
    del all_edges  # Free memory immediately
    
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
    
    # Optimize partitioning for your 2-VM setup (8 total CPU cores)
    num_partitions = min(8, len(edges) // 5000 + 1)  # Ensure at least 5000 edges per partition
    edges_per_partition = max(1000, len(edges) // num_partitions)
    edge_chunks = [edges[i:i + edges_per_partition] 
                   for i in range(0, len(edges), edges_per_partition)]
    
    print(f"Starting PageRank iterations with {num_partitions} partitions...")
    print(f"Edge chunks: {len(edge_chunks)}, average size: {len(edges) // len(edge_chunks) if edge_chunks else 0}")
    
    for iteration in range(max_iterations):
        print(f"Iteration {iteration + 1}/{max_iterations}")
        
        # Store current scores in Ray object store for efficient access
        current_scores_ref = ray.put(current_scores)
        out_degrees_ref = ray.put(out_degrees)
        
        # Distribute PageRank computation across chunks with controlled parallelism
        # Process in smaller batches to avoid overwhelming the cluster
        batch_size = min(4, len(edge_chunks))  # Process max 4 chunks at a time
        all_chunk_results = []
        
        for i in range(0, len(edge_chunks), batch_size):
            batch_chunks = edge_chunks[i:i + batch_size]
            print(f"  Processing batch {i//batch_size + 1}/{(len(edge_chunks) + batch_size - 1)//batch_size}")
            
            futures = [
                pagerank_update_chunk.remote(
                    chunk, current_scores_ref, out_degrees_ref, damping_factor, total_nodes
                ) for chunk in batch_chunks
            ]
            
            # Get results for this batch
            batch_results = ray.get(futures)
            all_chunk_results.extend(batch_results)
        
        # Merge results efficiently
        new_scores = {node: (1.0 - damping_factor) / total_nodes for node in nodes}
        
        for chunk_scores in all_chunk_results:
            for node, score in chunk_scores.items():
                new_scores[node] = score
        
        # Check for convergence with sampling for very large graphs
        if iteration > 0:
            # Sample nodes for convergence check if graph is very large
            sample_size = min(1000, total_nodes)
            sample_nodes = list(nodes)[:sample_size] if total_nodes > 1000 else nodes
            
            diff = sum((current_scores[node] - new_scores[node]) ** 2 
                      for node in sample_nodes)
            
            # Scale diff if we sampled
            if sample_size < total_nodes:
                diff = diff * (total_nodes / sample_size)
            
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
    
    # Initialize Ray - connect to existing cluster
    if not ray.is_initialized():
        ray.init(address="auto")
    
    # Print cluster information for debugging
    print(f"Ray cluster resources: {ray.cluster_resources()}")
    print(f"Ray cluster nodes: {len(ray.nodes())}")
    for node in ray.nodes():
        print(f"  Node: {node['NodeID'][:8]}... alive={node['Alive']} resources={node['Resources']}")
    
    try:
        print("Starting distributed PageRank with Ray...")
        convergence_iterations, top_nodes = pagerank_ray(config)
        
        end_time = time.time()
        display_results(config, start_time, end_time, convergence_iterations, top_nodes)
        
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
