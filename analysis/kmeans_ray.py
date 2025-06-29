import os
import time
import ray
import pyarrow.fs as fs
import pyarrow.csv as pv
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


def distributed_kmeans(config):
    hdfs = fs.HadoopFileSystem(host='o-master', port=54310)
    with hdfs.open_input_file(f'/data/{config["datafile"]}') as file:
        read_options = pv.ReadOptions(block_size=config["batch_size"])
        csv_reader = pv.open_csv(file, read_options=read_options)

        tasks = []
        for batch in csv_reader:
            df = batch.to_pandas()

            # Convert categoricals to numeric codes
            for col in df.columns:
                if df[col].dtype == object:
                    df[col] = df[col].astype('category').cat.codes

            # Fill any NaNs with -1
            df = df.fillna(-1)

            data = df.values
            tasks.append(ray_kmeans.remote(data, config))


        results = ray.get(tasks)
    return np.mean(results)


def main():
    start_time = time.time()
    ray.init(address='auto')
    end_time_system = time.time()

    config = {
        "datafile": "data_reddit_100M.csv",
        "n_clusters": 16,
        "num_nodes": get_num_nodes(),
        "cpus_per_node": 4,
        "batch_size": 1024 * 1024 * 20
    }

    res_score = distributed_kmeans(config)
    end_time = time.time()

    display_results(config, start_time, end_time, end_time_system, res_score)
    ray.shutdown()


if __name__ == "__main__":
    main()
