import os
import time
import ray
import argparse
import resource
import numpy as np
from collections import defaultdict

np.random.seed(42)


def display_results(config, start_time, end_time, centroids, sample_data):
    """Display and save K-Means results."""
    execution_time = end_time - start_time
    peak_memory = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # MB on Linux

    results_text = (
        f"\nDataset: {config['file']}"
        f"\nTotal execution time: {execution_time:.2f} seconds"
        f"\nPeak memory usage: {peak_memory:.2f} MB"
        f"\nNumber of clusters (K): {config['clusters']}"
        f"\nMaximum iterations: {config['max_iterations']}\n\nFinal Centroids:\n"
    )

    for i, centroid in enumerate(centroids):
        results_text += f"Cluster {i}: {centroid}\n"

    results_text += "\nSample clustered data:\n"
    for row in sample_data:
        results_text += f"Features: {row.features}, Cluster: {row.cluster}\n"

    print(results_text)

    os.makedirs("results", exist_ok=True)
    filename = f"results/kmeans_ray_results_{config['file'].replace('.csv', '')}_{int(time.time())}.txt"
    with open(filename, "w") as f:
        f.write(results_text)

    print(f"Results saved to {filename}")


def load_data_partition(file_path, start_row, chunk_size, features):
    """Load a partition of data into NumPy array."""
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
    return load_data_partition(file_path, start_row, chunk_size, features)


@ray.remote(num_cpus=1, scheduling_strategy="SPREAD")
def compute_partition_assignments(partition, centroids):
    """Assign clusters and compute partial sums/counts for a partition."""
    distances = np.linalg.norm(partition[:, np.newaxis] - centroids, axis=2)
    cluster_assignments = np.argmin(distances, axis=1)

    sums = defaultdict(lambda: np.zeros(centroids.shape[1], dtype=np.float32))
    counts = defaultdict(int)
    for point, cluster in zip(partition, cluster_assignments):
        sums[cluster] += point
        counts[cluster] += 1

    return dict(sums), dict(counts)


def kmeans_ray(config):
    """Distributed K-Means with Ray."""
    print("Starting distributed K-Means...")
    home_dir = os.path.expanduser("~")
    data_path = f"{home_dir}/project/data/{config['file']}"
    if not os.path.exists(data_path):
        data_path = f"../data/{config['file']}"
        if not os.path.exists(data_path):
            raise FileNotFoundError(f"Data file not found: {config['file']}")

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

    # Initialize centroids from sample
    init_data = partitions[0] if len(partitions[0]) >= config["clusters"] else np.vstack(partitions[:3])
    centroids = init_data[np.random.choice(len(init_data), config["clusters"], replace=False)]

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

        if shift < 1e-4:
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
    parser = argparse.ArgumentParser(description="Distributed K-Means with Ray")
    parser.add_argument("-f", "--file", required=True, help="CSV file name in data/ directory")
    parser.add_argument("-k", "--clusters", type=int, default=3, help="Number of clusters")
    parser.add_argument("--max-iterations", type=int, default=20, help="Maximum iterations")
    parser.add_argument("--batch-size", type=int, default=64*1024*1024, help="Batch read size (unused)")

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
