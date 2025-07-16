import time
import ray
import io
import os
import numpy as np
import pandas as pd
import argparse
import resource
from collections import defaultdict

np.random.seed(42)


def display_results(config, start_time, end_time, centroids, sample_data):
    execution_time = end_time - start_time
    peak_memory = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # MB on Linux
    
    results_text = f"""
Dataset: {config['datafile']}
Total execution time: {execution_time:.2f} seconds
Peak memory usage: {peak_memory:.2f} MB
Number of clusters (K): {config['k_clusters']}
Maximum iterations: {config['max_iterations']}

Final Centroids:
"""
    
    for i, centroid in enumerate(centroids):
        results_text += f"Cluster {i}: {centroid}\n"
    
    results_text += f"\nSample clustered data:\n"
    for row in sample_data:
        results_text += f"Features: {row.features}, Cluster: {row.cluster}\n"
    
    print(results_text)
    
    timestamp = int(time.time())
    filename = f'kmeans_ray_results_{config["datafile"].replace(".csv", "")}_{timestamp}.txt'
    if not os.path.exists('results'):
        os.makedirs('results')
    with open(f'results/{filename}', 'w') as f:
        f.write(results_text)
    
    print(f"Results saved to results/{filename}")


@ray.remote(num_cpus=1, memory=400*1024*1024, scheduling_strategy="SPREAD")
def process_data_chunk(data_chunk, features):
    # Process a chunk of data and extract features
    node_ip = ray._private.services.get_node_ip_address()
    print(f"(process_data_chunk) Processing chunk on node: {node_ip}")
    
    # Extract and convert features to float
    processed_data = []
    batch_size = 2000
    
    for i in range(0, len(data_chunk), batch_size):
        batch = data_chunk.iloc[i:i+batch_size]
        for _, row in batch.iterrows():
            # Extract features and convert to float, skip invalid rows
            try:
                feature_vector = [float(row[col]) for col in features]
                if not any(np.isnan(feature_vector)) and not any(np.isinf(feature_vector)):
                    processed_data.append(feature_vector)
            except (ValueError, TypeError):
                continue  # Skip invalid rows
    
    return np.array(processed_data)


@ray.remote(num_cpus=1, memory=300*1024*1024, scheduling_strategy="SPREAD")
def assign_clusters_chunk(data_chunk, centroids):
    # Assign clusters for a chunk of data points
    node_ip = ray._private.services.get_node_ip_address()
    print(f"(assign_clusters_chunk) Processing {len(data_chunk)} points on node: {node_ip}")
    
    if len(data_chunk) == 0:
        return [], []
    
    # Calculate distances to all centroids
    distances = np.linalg.norm(data_chunk[:, np.newaxis] - centroids, axis=2)
    cluster_assignments = np.argmin(distances, axis=1)
    
    return data_chunk.tolist(), cluster_assignments.tolist()


@ray.remote(num_cpus=1, memory=300*1024*1024, scheduling_strategy="SPREAD")
def compute_centroids_chunk(data_points, cluster_assignments, k_clusters):
    # Compute new centroids for assigned clusters in this chunk
    node_ip = ray._private.services.get_node_ip_address()
    print(f"(compute_centroids_chunk) Computing centroids on node: {node_ip}")
    
    cluster_sums = defaultdict(list)
    cluster_counts = defaultdict(int)
    
    # Group points by cluster
    for point, cluster in zip(data_points, cluster_assignments):
        cluster_sums[cluster].append(point)
        cluster_counts[cluster] += 1
    
    # Calculate partial centroids
    partial_centroids = {}
    for cluster_id, points in cluster_sums.items():
        if points:
            partial_centroids[cluster_id] = {
                'sum': np.sum(points, axis=0).tolist(),
                'count': len(points)
            }
    
    return partial_centroids


def load_and_prepare_data(config):
    data_path = f"../data/{config['datafile']}"

    if not os.path.exists(data_path):
        raise Exception(f"Data file not found : {data_path}")
    
    df = pd.read_csv(data_path)
    
    selected_features = ['FracSpecialChars', 'NumWords',
                        'AvgCharsPerSentence', 'AvgWordsPerSentence', 'AutomatedReadabilityIndex',
                        'SentimentPositive', 'SentimentNegative', 'SentimentCompound']
    
    print(f"Dataset statistics:")
    print(f"  Total rows: {len(df)}")
    print(f"  Total columns: {len(df.columns)}")
    print(f"  Selected features: {selected_features}")
    
    
    # Process data in chunks using Ray
    chunk_rows = 20000
    all_data = []
    chunk_count = 0
    
    for chunk_df in pd.read_csv(data_path, chunksize=chunk_rows):
        if len(chunk_df) == 0:
            continue
            
        chunk_count += 1
        print(f"Processing chunk {chunk_count} ({len(chunk_df)} rows)...")
        
        # Process single chunk
        future = process_data_chunk.remote(chunk_df, selected_features)
        chunk_data = ray.get(future)
        
        if len(chunk_data) > 0:
            all_data.append(chunk_data)
        
        # Progress update every 10 chunks
        if chunk_count % 10 == 0:
            total_points = sum(len(chunk) for chunk in all_data)
            print(f"  Progress: {total_points} valid data points so far")
        
        # Force garbage collection every 20 chunks
        if chunk_count % 20 == 0:
            import gc
            gc.collect()
    
    # If we already loaded the full dataframe, process it directly
    if not all_data and 'df' in locals():
        print("Processing full dataframe...")
        future = process_data_chunk.remote(df, selected_features)
        chunk_data = ray.get(future)
        if len(chunk_data) > 0:
            all_data.append(chunk_data)
    
    # Combine all data
    if not all_data:
        raise Exception("No valid data points found")
    
    combined_data = np.vstack(all_data)
    print(f"  Rows with valid features: {len(combined_data)}")
    
    return combined_data, selected_features


def kmeans_ray(config):
    # Distributed K-Means implementation using Ray
    print("Preparing data for K-Means clustering...")
    data, selected_features = load_and_prepare_data(config)
    
    K = config["k_clusters"]
    MAX_ITER = config["max_iterations"]
    
    # Initialize centroids by sampling K random points (same seed as Spark for reproducibility)
    np.random.seed(42)
    initial_indices = np.random.choice(len(data), K, replace=False)
    centroids = data[initial_indices].copy()
    
    print(f"Starting K-Means clustering with K={K}, max_iter={MAX_ITER}...")
    
    # Optimize partitioning for 2-VM setup
    num_partitions = 16
    points_per_partition = max(1000, len(data) // num_partitions)
    data_chunks = [data[i:i + points_per_partition] 
                   for i in range(0, len(data), points_per_partition)]
    
    print(f"Data partitioned into {len(data_chunks)} chunks, average size: {len(data) // len(data_chunks)}")
    
    for iteration in range(MAX_ITER):
        print(f"Iteration {iteration + 1}/{MAX_ITER}")
        
        # Store current centroids in Ray object store for efficient access
        centroids_ref = ray.put(centroids)
        
        # Step 1: Assign clusters with controlled parallelism
        batch_size = min(4, len(data_chunks))  # Process max 4 chunks at a time
        all_assignments = []
        all_points = []
        
        for i in range(0, len(data_chunks), batch_size):
            batch_chunks = data_chunks[i:i + batch_size]
            print(f"  Assigning clusters for batch {i//batch_size + 1}/{(len(data_chunks) + batch_size - 1)//batch_size}")
            
            futures = [
                assign_clusters_chunk.remote(chunk, centroids_ref)
                for chunk in batch_chunks
            ]
            
            batch_results = ray.get(futures)
            for points, assignments in batch_results:
                all_points.extend(points)
                all_assignments.extend(assignments)
        
        # Step 2: Compute new centroids
        print(f"  Computing new centroids...")
        
        # Partition assignments for centroid computation
        assignment_chunks = []
        points_chunks = []
        chunk_size = max(1000, len(all_points) // num_partitions)
        
        for i in range(0, len(all_points), chunk_size):
            points_chunks.append(all_points[i:i + chunk_size])
            assignment_chunks.append(all_assignments[i:i + chunk_size])
        
        # Compute partial centroids in parallel
        futures = [
            compute_centroids_chunk.remote(points, assignments, K)
            for points, assignments in zip(points_chunks, assignment_chunks)
        ]
        
        partial_results = ray.get(futures)
        
        # Merge partial centroids
        cluster_sums = defaultdict(lambda: np.zeros(len(selected_features)))
        cluster_counts = defaultdict(int)
        
        for partial_centroids in partial_results:
            for cluster_id, centroid_info in partial_centroids.items():
                cluster_sums[cluster_id] += np.array(centroid_info['sum'])
                cluster_counts[cluster_id] += centroid_info['count']
        
        # Calculate new centroids
        new_centroids = np.zeros_like(centroids)
        for cluster_id in range(K):
            if cluster_counts[cluster_id] > 0:
                new_centroids[cluster_id] = cluster_sums[cluster_id] / cluster_counts[cluster_id]
            else:
                # If cluster is empty, keep the old centroid
                new_centroids[cluster_id] = centroids[cluster_id]
        
        # Check for convergence
        centroid_shift = np.linalg.norm(centroids - new_centroids)
        print(f"  Centroid shift: {centroid_shift:.8f}")
        
        centroids = new_centroids
        
        if centroid_shift < 1e-4:
            print(f"Converged after {iteration + 1} iterations")
            break
    
    # Create sample data structure similar to Spark's Row format
    sample_indices = np.random.choice(len(all_points), min(5, len(all_points)), replace=False)
    
    # Create a simple class to mimic Spark's Row structure
    class Row:
        def __init__(self, features, cluster):
            self.features = features
            self.cluster = cluster
    
    sample_data = [Row(all_points[i], all_assignments[i]) for i in sample_indices]
    
    # Print cluster distribution (same format as Spark)
    cluster_distribution = defaultdict(int)
    for assignment in all_assignments:
        cluster_distribution[assignment] += 1
    
    print(f"\nCluster distribution:")
    for cluster_id in range(K):
        print(f"  Cluster {cluster_id}: {cluster_distribution[cluster_id]} points")
    
    return centroids, sample_data


def main():
    parser = argparse.ArgumentParser(description='Run distributed K-Means clustering using Ray')
    parser.add_argument('-f', '--file', 
                       required=True,
                       help='Name of the CSV file in local data/ directory')
    parser.add_argument('-k', '--clusters', type=int, default=3,
                       help='Number of clusters (default: 3)')
    parser.add_argument('--max-iterations', type=int, default=20,
                       help='Maximum number of iterations (default: 20)')
    parser.add_argument('--batch-size', type=int, default=64*1024*1024,
                       help='Batch size for reading data (default: 64MB)')
    
    args = parser.parse_args()
    
    config = {
        "datafile": args.file,
        "k_clusters": args.clusters,
        "max_iterations": args.max_iterations,
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
        print("Starting distributed K-Means clustering with Ray...")
        centroids, sample_data = kmeans_ray(config)
        
        end_time = time.time()
        display_results(config, start_time, end_time, centroids, sample_data)
        
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
