import time
import ray
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
    filename = f'pagerank_ray_results_{config["datafile"].replace(".csv", "")}_{timestamp}.txt'
    if not os.path.exists('results'):
        os.makedirs('results')
    with open(f'results/{filename}', 'w') as f:
        f.write(results_text)
    
    print(f"Results saved to results/{filename}")


@ray.remote(scheduling_strategy="SPREAD")
def build_adjacency_chunk(data_chunk):
    # Build adjacency lists from a chunk of data - keep distributed
    node_ip = ray._private.services.get_node_ip_address()
    print(f"(build_adjacency_chunk) Processing chunk on node: {node_ip}")
    
    adjacency = defaultdict(list)
    out_degrees = defaultdict(int)
    all_nodes = set()
    
    # Process in small batches to avoid memory issues
    batch_size = 1000
    for i in range(0, len(data_chunk), batch_size):
        batch = data_chunk.iloc[i:i+batch_size]
        for _, row in batch.iterrows():
            if pd.notna(row['SOURCE_SUBREDDIT']) and pd.notna(row['TARGET_SUBREDDIT']):
                source = str(row['SOURCE_SUBREDDIT'])
                target = str(row['TARGET_SUBREDDIT'])
                
                adjacency[source].append(target)
                out_degrees[source] += 1
                all_nodes.add(source)
                all_nodes.add(target)
    
    return dict(adjacency), dict(out_degrees), list(all_nodes)


@ray.remote(scheduling_strategy="SPREAD")
def pagerank_iteration_chunk(adjacency_chunk, scores_dict, out_degrees_dict, damping_factor, total_nodes):
    # Perform one PageRank iteration on a chunk of the adjacency list
    node_ip = ray._private.services.get_node_ip_address()
    print(f"(pagerank_iteration_chunk) Processing {len(adjacency_chunk)} nodes on node: {node_ip}")
    
    new_scores = defaultdict(float)
    
    # Initialize all nodes with base score
    for source_node, neighbors in adjacency_chunk.items():
        for target_node in neighbors:
            if target_node not in new_scores:
                new_scores[target_node] = (1.0 - damping_factor) / total_nodes
    
    # Add contributions from source nodes
    for source_node, neighbors in adjacency_chunk.items():
        if source_node in out_degrees_dict and out_degrees_dict[source_node] > 0:
            contribution = scores_dict.get(source_node, 1.0 / total_nodes) / out_degrees_dict[source_node]
            
            # Distribute contribution to all neighbors
            for target_node in neighbors:
                new_scores[target_node] += damping_factor * contribution
    
    return dict(new_scores)


def build_distributed_graph(config):
    # Build distributed adjacency lists using Ray with streaming approach
    data_path = f"../data/{config['datafile']}"
    
    if not os.path.exists(data_path):
        raise Exception(f"Data file not found: {data_path}")
    
    print(f"Reading data from {data_path}...")
    
    # Stream data in very small chunks to avoid memory issues
    chunk_rows = 10000
    all_adjacency_refs = []
    all_out_degrees = defaultdict(int)
    all_nodes = set()
    
    chunk_count = 0
    
    # Process chunks one by one to avoid loading entire dataset into memory
    for chunk_df in pd.read_csv(data_path, chunksize=chunk_rows):
        if len(chunk_df) == 0:
            continue
            
        chunk_count += 1
        print(f"Processing chunk {chunk_count} ({len(chunk_df)} rows)...")
        
        # Process single chunk immediately
        future = build_adjacency_chunk.remote(chunk_df)
        adjacency, out_degrees, nodes = ray.get(future)
        
        # Store adjacency as Ray object (keep distributed)
        if adjacency:  # Only store non-empty adjacency lists
            adj_ref = ray.put(adjacency)
            all_adjacency_refs.append(adj_ref)
        
        # Combine metadata
        for node, degree in out_degrees.items():
            all_out_degrees[node] += degree
        all_nodes.update(nodes)
        
        # Clear local memory immediately
        del chunk_df, future, adjacency, out_degrees, nodes
        
        # Progress update every 50 chunks
        if chunk_count % 50 == 0:
            print(f"  Progress: {len(all_nodes)} total nodes, {len(all_adjacency_refs)} adjacency chunks")
            
            # Force garbage collection every 50 chunks
            import gc
            gc.collect()
    
    print(f"Graph statistics:")
    print(f"  Total nodes: {len(all_nodes)}")
    print(f"  Total chunks processed: {chunk_count}")
    print(f"  Adjacency chunks: {len(all_adjacency_refs)}")
    
    return all_adjacency_refs, dict(all_out_degrees), list(all_nodes)


def pagerank_ray(config):
    # Distributed PageRank implementation using Ray
    print("Building distributed graph structure...")
    adjacency_refs, out_degrees, all_nodes = build_distributed_graph(config)
    
    # Initialize PageRank scores
    total_nodes = len(all_nodes)
    initial_score = 1.0 / total_nodes
    current_scores = {node: initial_score for node in all_nodes}
    
    damping_factor = config["damping_factor"]
    convergence_threshold = config["convergence_threshold"]
    max_iterations = config["max_iterations"]
    
    print(f"Starting PageRank iterations with {len(adjacency_refs)} distributed chunks...")
    print(f"Total nodes: {total_nodes}")
    
    for iteration in range(max_iterations):
        print(f"Iteration {iteration + 1}/{max_iterations}")
        
        # Store current scores and out_degrees in Ray object store
        current_scores_ref = ray.put(current_scores)
        out_degrees_ref = ray.put(out_degrees)
        
        # Process adjacency chunks in small batches to control memory
        batch_size = 2  # Process max 2 chunks at a time
        all_chunk_results = []
        
        for i in range(0, len(adjacency_refs), batch_size):
            batch_refs = adjacency_refs[i:i + batch_size]
            print(f"  Processing batch {i//batch_size + 1}/{(len(adjacency_refs) + batch_size - 1)//batch_size}")
            
            futures = [
                pagerank_iteration_chunk.remote(
                    adj_ref, current_scores_ref, out_degrees_ref, damping_factor, total_nodes
                ) for adj_ref in batch_refs
            ]
            
            # Get results for this batch
            batch_results = ray.get(futures)
            all_chunk_results.extend(batch_results)
        
        # Merge results efficiently
        new_scores = {node: (1.0 - damping_factor) / total_nodes for node in all_nodes}
        
        for chunk_scores in all_chunk_results:
            for node, score in chunk_scores.items():
                new_scores[node] = score
        
        # Check for convergence
        if iteration > 0:
            # Sample nodes for convergence check if graph is very large
            sample_size = min(1000, total_nodes)
            sample_nodes = list(all_nodes)[:sample_size] if total_nodes > 1000 else all_nodes
            
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
                       help='Name of the CSV file in local data/ directory')
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
