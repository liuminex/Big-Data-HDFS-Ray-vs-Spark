import argparse
import os
import resource
import time
from collections import defaultdict

import numpy as np
import pandas as pd
import ray


def display_results(config, start_time, end_time, convergence_iterations, top_nodes):
    """Display and save PageRank results to console and file."""
    execution_time = end_time - start_time
    peak_memory_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    
    # Format top nodes output
    top_nodes_output = f"\nTop {len(top_nodes)} Nodes by PageRank Score:"
    for i, (node, score) in enumerate(top_nodes, 1):
        top_nodes_output += f"\n{i:3d}. {node}: {score:.6f}"
    
    # Display formatted results
    results_header = "PAGERANK ALGORITHM RESULTS (RAY)"
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
• Graph representation: Adjacency lists (distributed)
• Isolated node handling: Base score initialization
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
    filename = f'pagerank_ray_results_{dataset_name}_{timestamp}.txt'
    filepath = os.path.join(results_dir, filename)
    
    # Save results to file
    with open(filepath, 'w') as f:
        f.write(results_text)
    
    print(f"Results saved to {filepath}")
    print(f"{'=' * 60}")


@ray.remote(scheduling_strategy="SPREAD")
def build_adjacency_chunk(data_chunk):
    """Build adjacency lists and node statistics from a data chunk."""
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
    """Perform one PageRank iteration on an adjacency chunk."""
    # Perform one PageRank iteration on a chunk of the adjacency list
    node_ip = ray._private.services.get_node_ip_address()
    print(f"(pagerank_iteration_chunk) Processing {len(adjacency_chunk)} nodes on node: {node_ip}")
    
    new_scores = {}
    
    # Initialize all target nodes with base score (isolated node handling)
    for source_node, neighbors in adjacency_chunk.items():
        for target_node in neighbors:
            if target_node not in new_scores:
                new_scores[target_node] = (1.0 - damping_factor) / total_nodes
    
    # Add contributions from source nodes
    for source_node, neighbors in adjacency_chunk.items():
        if source_node in out_degrees_dict and out_degrees_dict[source_node] > 0:
            source_score = scores_dict.get(source_node, 1.0 / total_nodes)
            contribution = source_score * damping_factor / out_degrees_dict[source_node]
            
            # Distribute contribution to all neighbors
            for target_node in neighbors:
                new_scores[target_node] += contribution
    
    return new_scores


def build_distributed_graph(config):
    """Build distributed graph structure from CSV data using Ray workers."""
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
    """Execute distributed PageRank algorithm using Ray."""
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
    
    # Standardized convergence parameters
    convergence_sample_size = min(config.get("convergence_sample_size", 10000), total_nodes)
    convergence_sample_fraction = convergence_sample_size / total_nodes
    
    print(f"PageRank configuration:")
    print(f"  Total nodes: {total_nodes}")
    print(f"  Damping factor: {damping_factor}")
    print(f"  Convergence threshold: {convergence_threshold}")
    print(f"  Convergence sample size: {convergence_sample_size}")
    print(f"  Max iterations: {max_iterations}")
    
    print(f"Starting PageRank iterations with {len(adjacency_refs)} distributed chunks...")
    
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
        
        # Merge results efficiently - handle isolated nodes properly
        new_scores = {}
        
        # Initialize all nodes with base score (handles isolated nodes)
        for node in all_nodes:
            new_scores[node] = (1.0 - damping_factor) / total_nodes
        
        # Add contributions from chunks
        for chunk_scores in all_chunk_results:
            for node, score in chunk_scores.items():
                if node in new_scores:
                    # Replace base score with computed score (includes base + contributions)
                    new_scores[node] = score
        
        # Standardized convergence check
        if iteration > 0:
            # Use deterministic sampling for convergence check
            np.random.seed(42)  # Fixed seed for reproducible sampling
            if convergence_sample_size < total_nodes:
                # Random sampling of nodes for large graphs
                sample_nodes = np.random.choice(list(all_nodes), convergence_sample_size, replace=False)
            else:
                sample_nodes = list(all_nodes)
            
            # Calculate L2 norm of differences
            diff = sum((current_scores[node] - new_scores[node]) ** 2 for node in sample_nodes)
            
            # Scale diff to represent full graph if we sampled
            if convergence_sample_size < total_nodes:
                diff = diff / convergence_sample_fraction
            
            print(f"  Convergence metric (L2 norm): {diff:.8f}")
            print(f"  Sample size: {len(sample_nodes)} / {total_nodes}")
            
            if diff < convergence_threshold:
                print(f"Converged after {iteration + 1} iterations")
                break
        
        current_scores = new_scores
    
    # Get top nodes by PageRank score
    sorted_nodes = sorted(current_scores.items(), key=lambda x: x[1], reverse=True)
    top_nodes = sorted_nodes[:10]
    
    return iteration + 1, top_nodes


def main():
    """Parse arguments and run PageRank benchmark with Ray."""
    parser = argparse.ArgumentParser(description='Distributed PageRank using Ray')
    parser.add_argument('-f', '--datafile', type=str, required=True,
                       help='Input CSV file name in data/ directory')
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
