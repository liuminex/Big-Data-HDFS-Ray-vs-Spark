import sys
import os
import time
import argparse
import resource
import pandas as pd
import ray
from collections import defaultdict

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
    filename = f'triangles_ray_results_{os.path.basename(config["file"]).replace(".csv", "")}_{timestamp}.txt'
    if not os.path.exists('results'):
        os.makedirs('results')
    with open(f'results/{filename}', 'w') as f:
        f.write(results_text)
    
    print(f"Results saved to results/{filename}")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Count triangles in a graph using Ray')
    parser.add_argument('-f', '--file', type=str, default='data_reddit_100M.csv',
                        help='Path to the input CSV file')
    parser.add_argument('-c', '--chunks', type=int, default=4,
                        help='Number of chunks to split the nodes into')
    args = parser.parse_args()

    # Initialize Ray first - connect to existing cluster
    ray.init(address='auto')
    
    # Print cluster information
    print(f"Ray cluster nodes: {len(ray.nodes())}")
    print(f"Ray cluster resources: {ray.cluster_resources()}")
    for node in ray.nodes():
        print(f"  Node: {node['NodeID'][:8]}... alive={node['Alive']} resources={node['Resources']}")

    # Count total rows first without loading all data
    print(f"Analyzing file ../data/{args.file} to determine chunking strategy...")
    total_rows = 0
    with open(f'../data/{args.file}', 'r') as file:
        next(file)  # Skip header
        for line in file:
            total_rows += 1
            if total_rows % 1000000 == 0:
                print(f"  Counted {total_rows} rows so far...")
    
    print(f"Total rows in dataset: {total_rows}")
    
    # Calculate optimal chunking - more cores per task for better memory distribution
    total_cores = int(ray.cluster_resources().get('CPU', 12))
    rows_per_chunk = max(50000, total_rows // total_cores)  # Each chunk gets multiple cores
    estimated_chunks = max(1, total_rows // rows_per_chunk)
    
    print(f"Will process ~{estimated_chunks} chunks with ~{rows_per_chunk} rows each")
    print(f"Using {total_cores} total cores across cluster")

    @ray.remote(num_cpus=2, scheduling_strategy="SPREAD")
    def process_file_chunk(file_path, start_row, chunk_size):
       
        try:
            node_ip = ray._private.services.get_node_ip_address()
            print(f"Processing rows {start_row}-{start_row + chunk_size} on node: {node_ip}")
        except:
            print(f"Processing rows {start_row}-{start_row + chunk_size}")
        
        home_dir = os.path.expanduser('~')
        tmp = f'{home_dir}/project/data/{os.path.basename(file_path)}'
        file_path = tmp
        
        adjacency = defaultdict(set)
        edge_count = 0
        
        try:
            with open(file_path, 'r') as file:
                next(file)  # Skip header
                
                # Read ALL edges to build complete adjacency
                for line in file:
                    parts = line.strip().split(',')
                    if len(parts) >= 2:
                        src, dst = str(parts[0]), str(parts[1])
                        if src != dst:  # Skip self-loops
                            adjacency[src].add(dst)
                            edge_count += 1
        except Exception as e:
            print(f"Error reading complete file {file_path}: {e}")
            return 0, 0
            
        print(f"  Worker built complete adjacency: {len(adjacency)} nodes from {edge_count} total edges")
        
        # Now process only THIS chunk's edges for triangle counting
        edge_list = []
        try:
            with open(file_path, 'r') as file:
                next(file)  # Skip header
                
                # Skip to start_row
                for _ in range(start_row):
                    try:
                        next(file)
                    except StopIteration:
                        print(f"Reached end of file while skipping to start_row {start_row}")
                        return 0, 0
                
                # Collect only THIS chunk's edges
                for _ in range(chunk_size):
                    try:
                        line = next(file).strip()
                        parts = line.split(',')
                        if len(parts) >= 2:
                            src, dst = str(parts[0]), str(parts[1])
                            if src != dst:  # Skip self-loops
                                edge_list.append((src, dst))
                    except StopIteration:
                        break
        except Exception as e:
            print(f"Error reading chunk {file_path}: {e}")
            return 0, 0
        
        print(f"  Worker processing {len(edge_list)} edges from chunk {start_row}-{start_row + len(edge_list)}")
        
        # Count triangles using adjacency but only THIS chunk's edges
        total_triangles = 0
        processed = 0
        
        for src, dst in edge_list:
            # Get neighbors from adjacency
            src_neighbors = adjacency.get(src, set())
            dst_neighbors = adjacency.get(dst, set())
            
            # Find common neighbors (triangles)
            if src_neighbors and dst_neighbors:
                common_neighbors = len(src_neighbors & dst_neighbors)
                total_triangles += common_neighbors
            
            processed += 1
            if processed % 10000 == 0:
                print(f"    Processed {processed}/{len(edge_list)} edges on this worker")
        
        return total_triangles, len(edge_list)

    start_time = time.time()
    

    home_dir = os.path.expanduser('~')
    file_path = f'{home_dir}/project/data/{args.file}'
    print(f"Using file path: {file_path}")
    
    # Verify file exists on master
    if not os.path.exists(file_path):
        print(f"ERROR: File {file_path} not found on master node!")
        # Try the relative path as fallback
        fallback_path = os.path.abspath(f'../data/{args.file}')
        if os.path.exists(fallback_path):
            file_path = fallback_path
            print(f"Using fallback path: {file_path}")
        else:
            print(f"File not found in either location!")
            ray.shutdown()
            return

    # Submit tasks that process file chunks directly - no master memory loading!
    print(f"Submitting file processing tasks directly to workers...")
    futures = []
    chunk_info = []
    
    current_row = 0
    chunk_id = 0
    
    while current_row < total_rows:
        chunk_size = min(rows_per_chunk, total_rows - current_row)
        
        future = process_file_chunk.remote(file_path, current_row, chunk_size)
        futures.append(future)
        chunk_info.append((current_row, chunk_size))
        
        print(f"  Submitted chunk {chunk_id + 1}: rows {current_row}-{current_row + chunk_size}")
        
        current_row += chunk_size
        chunk_id += 1
        
        # Process in small batches to avoid overwhelming
        if len(futures) >= 6 or current_row >= total_rows:  # 6 tasks = 3 nodes * 2 tasks
            print(f"  Processing batch of {len(futures)} tasks...")
            batch_results = ray.get(futures)
            
            # Process results
            for result in batch_results:
                triangles, edges = result
                print(f"    Chunk completed: {triangles} triangles from {edges} edges")
            
            # Clear batch
            futures = []
    
    print("All file chunks processed successfully!")
    

    print(f"Collecting final results...")
    all_futures = []
    all_results = []
    
    current_row = 0
    chunk_id = 0
    
    while current_row < total_rows:
        chunk_size = min(rows_per_chunk, total_rows - current_row)
        
        future = process_file_chunk.remote(file_path, current_row, chunk_size)
        all_futures.append(future)
        
        current_row += chunk_size
        chunk_id += 1
        
        # Process in batches of 6
        if len(all_futures) >= 6 or current_row >= total_rows:
            batch_results = ray.get(all_futures)
            all_results.extend(batch_results)
            all_futures = []
    
    end_time = time.time()
    
    # Extract triangle counts and calculate total
    triangle_counts = [result[0] for result in all_results]
    total_edges_processed = sum(result[1] for result in all_results)
    
    # Calculate total triangles
    total_triangles = sum(triangle_counts)
    
    print(f"Processed {total_edges_processed} total edges")
    print(f"Found {total_triangles} triangles")
    
    # Create config dictionary for display_results
    config = {
        'file': args.file,
        'chunks': len(all_results)
    }
    
    # Display and save results
    display_results(config, start_time, end_time, triangle_counts, total_triangles)
    
    ray.shutdown()

if __name__ == "__main__":
    main()
