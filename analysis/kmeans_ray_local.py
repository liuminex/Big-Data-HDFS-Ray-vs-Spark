import os
import time
import ray
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import calinski_harabasz_score

def get_num_nodes():
    return sum(1 for node in ray.nodes() if node['Alive'])

def display_results(config, start_time, end_time, end_time_system, calinski_harabasz_res):
    with open('results.txt', 'w') as f:
        f.write(f"Config: {config}\n")
        f.write(f"Start Time: {start_time}\n")
        f.write(f"End Time: {end_time}\n")
        f.write(f"System Init Time: {end_time_system}\n")
        f.write(f"Calinski-Harabasz Score: {calinski_harabasz_res}\n")

@ray.remote
def ray_kmeans(data_batch, config):
    node_ip = ray._private.services.get_node_ip_address()
    print(f"(ray_kmeans) Executing on node with IP: {node_ip}")
    kmeans = KMeans(n_clusters=config["n_clusters"], random_state=42)
    kmeans.fit(data_batch)
    return calinski_harabasz_score(data_batch, kmeans.labels_)

def distributed_kmeans_local(config):
    # Read the whole CSV into a DataFrame
    df = pd.read_csv(config["local_path"])

    # Convert categoricals to numeric codes
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype('category').cat.codes

    # Fill NaNs
    df = df.fillna(-1)

    data = df.values
    batch_size_rows = config["batch_size_rows"]

    # Split into batches of rows
    tasks = []
    num_batches = (len(data) + batch_size_rows - 1) // batch_size_rows
    for i in range(num_batches):
        start_idx = i * batch_size_rows
        end_idx = min((i + 1) * batch_size_rows, len(data))
        batch = data[start_idx:end_idx]
        tasks.append(ray_kmeans.remote(batch, config))

    results = ray.get(tasks)
    return np.mean(results)

def main():
    start_time = time.time()
    ray.init(address='auto')
    end_time_system = time.time()

    config = {
        "local_path": "/data/data_reddit_100M.csv",
        "n_clusters": 16,
        "num_nodes": get_num_nodes(),
        "cpus_per_node": 4,
        "batch_size_rows": 50000  # Adjust depending on your memory
    }

    res_score = distributed_kmeans_local(config)
    end_time = time.time()

    display_results(config, start_time, end_time, end_time_system, res_score)
    ray.shutdown()

if __name__ == "__main__":
    main()
