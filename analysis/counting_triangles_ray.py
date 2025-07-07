from memory_profiler import memory_usage
import networkx as nx
import subprocess
import time
import sys
import ray
import argparse
import os
import resource

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
    filename = f'counting_triangles_ray_results_{os.path.basename(config["file"]).replace(".csv", "")}_{timestamp}.txt'
    if not os.path.exists('results'):
        os.makedirs('results')
    with open(f'results/{filename}', 'w') as f:
        f.write(results_text)
    
    print(f"Results saved to results/{filename}")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Count triangles in a graph using Ray')
    parser.add_argument('-f', '--file', type=str, default='../data/data_reddit_100M.csv',
                        help='Path to the input CSV file')
    parser.add_argument('-c', '--chunks', type=int, default=8,
                        help='Number of chunks to split the nodes into')
    args = parser.parse_args()

    # Load from local file
    lines = []
    skip_headers = True

    with open(args.file, 'r') as file:
        for raw_line in file:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if skip_headers:
                skip_headers = False
                continue
            parts = line.split(",")
            if len(parts) < 2:
                continue
            lines.append(f"{parts[0]} {parts[1]}")

    G = nx.parse_edgelist(lines, nodetype=str, create_using=nx.DiGraph())
    G = G.to_undirected()

    num_chunks = args.chunks

    node_list = list(G.nodes())
    node_chunk_size = len(node_list) // num_chunks

    node_chunks = [
        node_list[node_chunk_size * i : node_chunk_size * (i + 1)] if i < num_chunks - 1
        else node_list[node_chunk_size * i :]
        for i in range(num_chunks)
    ]

    ray.init()

    @ray.remote
    def compute_triangles(G, nodes):
        return nx.triangles(G, nodes=nodes)

    A = time.time()
    G_ref = ray.put(G)

    results = []
    for chunk in node_chunks:
        result = ray.get(compute_triangles.remote(G_ref, chunk))
        results.append(result)

    end_time = time.time()
    
    # Calculate total triangles
    total_triangles = sum([sum(result.values()) if isinstance(result, dict) else result for result in results])
    
    # Create config dictionary for display_results
    config = {
        'file': args.file,
        'chunks': args.chunks
    }
    
    # Display and save results
    display_results(config, A, end_time, results, total_triangles)
    
    ray.shutdown()

# Run main with peak memory measurement
mem_usage = memory_usage(main, interval=0.1, max_usage=True)
print(f"Peak memory usage: {mem_usage} MB")
