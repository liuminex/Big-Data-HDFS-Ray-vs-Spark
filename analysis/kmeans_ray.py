import argparse
import os
import resource
import time
from collections import defaultdict

import numpy as np
import ray


def display_results(config, start_time, end_time, centroids, sample_data):
    """Display and save K-Means clustering results to console and file."""
    execution_time = end_time - start_time
    peak_memory_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    
    # Format centroids output
    centroids_output = "\nFinal Cluster Centroids:"
    for i, centroid in enumerate(centroids):
        centroids_output += f"\nCluster {i}: [{', '.join(f'{x:.4f}' for x in centroid)}]"
    
    # Format sample data output
    sample_output = "\nSample Clustered Data:"
    for row in sample_data[:5]:  # Limit to first 5 samples
        features_str = '[' + ', '.join(f'{x:.4f}' for x in row.features) + ']'
        sample_output += f"\n  Features: {features_str} -> Cluster: {row.cluster}"
    
    # Display formatted results
    results_header = "K-MEANS CLUSTERING RESULTS (RAY)"
    results_text = f"""
{'=' * 60}
{results_header:^60}
{'=' * 60}
Dataset: {config['datafile']}
Execution time: {execution_time:.2f} seconds
Peak memory usage: {peak_memory_mb:.2f} MB
Number of clusters (K): {config['clusters']}
Maximum iterations: {config['max_iterations']}

Algorithm Configuration:
• Clustering method: K-Means with Lloyd's algorithm
• Convergence criterion: Centroid shift < {config.get('convergence_tolerance', 1e-4)}
• Initialization: k-means++
• Random seed: {config.get('random_seed', 42)}
• Features used: 8 text analysis features
{centroids_output}
{sample_output}
{'=' * 60}
"""
    
    print(results_text)
    
    # Create results directory if it doesn't exist
    results_dir = 'results'
    os.makedirs(results_dir, exist_ok=True)
    
    # Generate standardized filename with timestamp
    timestamp = int(time.time())
    dataset_name = os.path.basename(config['datafile']).replace('.csv', '')
    filename = f'kmeans_ray_results_{dataset_name}_{timestamp}.txt'
    filepath = os.path.join(results_dir, filename)
    
    # Save results to file
    with open(filepath, 'w') as f:
        f.write(results_text)
    
    print(f"Results saved to {filepath}")
    print(f"{'=' * 60}")


def kmeans_plus_plus_init(data_points, k, random_state=42):
    """Initialize centroids using k-means++ algorithm for better convergence."""
    np.random.seed(random_state)
    
    n_samples, n_features = data_points.shape
    centroids = np.zeros((k, n_features))
    
    # Choose first centroid randomly
    centroids[0] = data_points[np.random.randint(n_samples)]
    
    # Choose remaining centroids using k-means++ method
    for i in range(1, k):
        # Calculate distances to nearest centroid for each point
        distances = np.array([min([np.linalg.norm(x - c)**2 for c in centroids[:i]]) for x in data_points])
        
        # Choose next centroid with probability proportional to squared distance
        probabilities = distances / distances.sum()
        cumulative_probs = probabilities.cumsum()
        r = np.random.rand()
        
        for j, p in enumerate(cumulative_probs):
            if r < p:
                centroids[i] = data_points[j]
                break
    
    return centroids


def load_data_partition(file_path, start_row, chunk_size, features):
    """Load a specific partition of CSV data into NumPy array."""
    data_points = []
    with open(file_path, "r") as file:
        header = next(file).strip().split(",")
        feature_indices = [header.index(f) for f in features if f in header]

        # Skip to start row
        for _ in range(start_row):
            try:
                next(file)
            except StopIteration:
                return np.array([], dtype=np.float32)

        # Read chunk
        for _ in range(chunk_size):
            try:
                parts = next(file).strip().split(",")
                if len(parts) > max(feature_indices):
                    feature_vector = []
                    for idx in feature_indices:
                        try:
                            val = float(parts[idx])
                            if np.isnan(val) or np.isinf(val):
                                break
                            feature_vector.append(val)
                        except ValueError:
                            break
                    if len(feature_vector) == len(features):
                        data_points.append(feature_vector)
            except StopIteration:
                break
    return np.array(data_points, dtype=np.float32)


@ray.remote
def load_data_partition_remote(file_path, start_row, chunk_size, features):
    """Remote wrapper for loading data partitions in Ray workers."""
    return load_data_partition(file_path, start_row, chunk_size, features)


@ray.remote(num_cpus=1, scheduling_strategy="SPREAD")
def compute_partition_assignments(partition, centroids):
    """Compute cluster assignments and partial statistics for a data partition."""
    distances = np.linalg.norm(partition[:, np.newaxis] - centroids, axis=2)
    cluster_assignments = np.argmin(distances, axis=1)

    sums = defaultdict(lambda: np.zeros(centroids.shape[1], dtype=np.float32))
    counts = defaultdict(int)
    for point, cluster in zip(partition, cluster_assignments):
        sums[cluster] += point
        counts[cluster] += 1

    return dict(sums), dict(counts)


def kmeans_ray(config):
    """Execute distributed K-Means clustering using Ray."""
    print("Starting distributed K-Means...")
    
    # Set random seed for reproducibility
    np.random.seed(config.get('random_seed', 42))
    
    home_dir = os.path.expanduser("~")
    data_path = f"{home_dir}/project/data/{config['datafile']}"
    if not os.path.exists(data_path):
        data_path = f"../data/{config['datafile']}"
        if not os.path.exists(data_path):
            raise FileNotFoundError(f"Data file not found: {config['datafile']}")

    features = [
        "FracSpecialChars", "NumWords", "AvgCharsPerSentence", "AvgWordsPerSentence",
        "AutomatedReadabilityIndex", "SentimentPositive", "SentimentNegative", "SentimentCompound"
    ]

    # Load partitions in parallel
    rows_per_partition = 400_000
    total_rows = 20_824_400  # Pre-known; could be counted dynamically
    num_partitions = (total_rows + rows_per_partition - 1) // rows_per_partition

    print(f"Loading {num_partitions} partitions of ~{rows_per_partition} rows...")
    load_tasks = [
        load_data_partition_remote.remote(data_path, i * rows_per_partition,
                                          min(rows_per_partition, total_rows - i * rows_per_partition),
                                          features)
        for i in range(num_partitions)
    ]

    partitions = [p for p in ray.get(load_tasks) if len(p) > 0]
    if not partitions:
        raise ValueError("No valid data points loaded.")

    partition_refs = [ray.put(p) for p in partitions]
    print(f"Loaded {len(partition_refs)} partitions with {sum(len(p) for p in partitions)} points.")

    # Initialize centroids using k-means++ for better convergence
    init_data = partitions[0] if len(partitions[0]) >= config["clusters"] else np.vstack(partitions[:3])
    centroids = kmeans_plus_plus_init(init_data, config["clusters"], config.get('random_seed', 42))

    # Set convergence tolerance
    convergence_tolerance = config.get('convergence_tolerance', 1e-4)
    
    print(f"K-Means configuration:")
    print(f"  Clusters: {config['clusters']}")
    print(f"  Max iterations: {config['max_iterations']}")
    print(f"  Convergence tolerance: {convergence_tolerance}")
    print(f"  Random seed: {config.get('random_seed', 42)}")
    print(f"  Initialization: k-means++")

    # Run K-Means
    for iteration in range(config["max_iterations"]):
        print(f"Iteration {iteration + 1}/{config['max_iterations']}")
        centroid_ref = ray.put(centroids)

        assignment_tasks = [compute_partition_assignments.remote(p, centroid_ref) for p in partition_refs]
        results = ray.get(assignment_tasks)

        sums = defaultdict(lambda: np.zeros(centroids.shape[1], dtype=np.float32))
        counts = defaultdict(int)
        for part_sums, part_counts in results:
            for cid, vec in part_sums.items():
                sums[cid] += vec
                counts[cid] += part_counts[cid]

        new_centroids = np.array([
            sums[c] / counts[c] if counts[c] > 0 else centroids[c]
            for c in range(config["clusters"])
        ])

        shift = np.linalg.norm(centroids - new_centroids)
        print(f"  Centroid shift: {shift:.8f}")
        centroids = new_centroids

        if shift < convergence_tolerance:
            print(f"Converged after {iteration + 1} iterations.")
            break

    # Prepare sample output
    class Row:
        def __init__(self, features, cluster):
            self.features = features
            self.cluster = cluster

    sample_points = partitions[0][np.random.choice(len(partitions[0]), min(5, len(partitions[0])), replace=False)]
    sample_data = [
        Row(point.tolist(), int(np.argmin(np.linalg.norm(point - centroids, axis=1))))
        for point in sample_points
    ]

    return centroids, sample_data


def main():
    """Parse arguments and run K-Means benchmark with Ray."""
    parser = argparse.ArgumentParser(description="Distributed K-Means clustering using Ray")
    parser.add_argument("-f", "--datafile", type=str, required=True, 
                       help="Input CSV file name in data/ directory")
    parser.add_argument("-k", "--clusters", type=int, default=3, 
                       help="Number of clusters for K-Means")
    parser.add_argument("--max-iterations", type=int, default=20, 
                       help="Maximum number of iterations")
    parser.add_argument("--convergence-tolerance", type=float, default=1e-4,
                       help="Convergence tolerance for centroid shift")
    parser.add_argument("--random-seed", type=int, default=42,
                       help="Random seed for reproducibility")

    args = parser.parse_args()
    config = vars(args)

    start_time = time.time()
    ray.init(address="auto")

    print(f"Ray cluster resources: {ray.cluster_resources()}")
    centroids, sample_data = kmeans_ray(config)
    end_time = time.time()
    display_results(config, start_time, end_time, centroids, sample_data)


if __name__ == "__main__":
    main()
